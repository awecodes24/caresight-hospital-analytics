"""
job_scheduler.py

This file is like an ALARM CLOCK for our program.
It automatically runs certain tasks at certain times,
just like how your phone alarm wakes you up every morning.
"""

import signal   # helps us detect when someone presses Ctrl+C or stops the program
import sys      # helps us exit the program cleanly

# APScheduler is a library that works like a timer/alarm clock
# We wrap it in try/except just in case someone forgot to install it
try:
    from apscheduler.schedulers.blocking import BlockingScheduler  # the main alarm clock
    from apscheduler.triggers.cron import CronTrigger              # lets us set WHEN the alarm rings
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False

# These are OUR OWN files sitting inside the logging_monitoring folder
from logging_monitoring.logger import setup_logger   # for printing logs like "job started"
from logging_monitoring.monitor import PipelineMonitor  # tracks if a job passed or failed
from logging_monitoring.alerter import Alerter          # sends alerts if something breaks

logger = setup_logger(__name__)  # start the logger for this file


class JobScheduler:
    """
    This is our alarm clock class.
    It sets 3 alarms:
      1. ETL job      - processes data (most important)
      2. Report job   - generates reports
      3. Cleanup job  - deletes old files
    """

    def __init__(self, config):
        """
        This runs ONCE when we first create the scheduler.
        It sets everything up before any job runs.
        """
        self.config = config                               # our settings (like what time to run jobs)
        self.monitor = PipelineMonitor()                   # tracks job results
        self.alerter = Alerter(config)                     # sends alerts on failure
        self.scheduler = BlockingScheduler(timezone="UTC") # create the alarm clock (uses UTC time)
        self._register_jobs()    # set the alarms
        self._register_signals() # listen for Ctrl+C so we can stop cleanly

    # ---------------------------------------------------------------
    # SETTING THE ALARMS
    # ---------------------------------------------------------------

    def _register_jobs(self):
        """
        This is where we SET each alarm with a time.
        The times come from our config file (so they're easy to change).
        Cron is just a way to write time schedules, like "run every day at 9am".
        """
        sc = self.config.scheduler  # grab the schedule settings from config

        # Alarm 1 - ETL job (data processing)
        self.scheduler.add_job(
            func=self._run_etl_job,                          # what to run when alarm rings
            trigger=CronTrigger.from_crontab(sc.etl_cron),  # when to run it
            id="etl_pipeline",                               # give it a name/id
            name="Daily ETL Pipeline",
            max_instances=1,         # don't run it twice at the same time
            misfire_grace_time=600,  # if it missed the alarm, still run it within 10 mins
            replace_existing=True,
        )
        logger.info(f"ETL job scheduled: {sc.etl_cron}")

        # Alarm 2 - Report job (weekly reports)
        self.scheduler.add_job(
            func=self._run_report_job,
            trigger=CronTrigger.from_crontab(sc.report_cron),
            id="report_generation",
            name="Weekly Report Generation",
            max_instances=1,
            misfire_grace_time=600,
            replace_existing=True,
        )
        logger.info(f"Report job scheduled: {sc.report_cron}")

        # Alarm 3 - Cleanup job (delete old files)
        self.scheduler.add_job(
            func=self._run_cleanup_job,
            trigger=CronTrigger.from_crontab(sc.cleanup_cron),
            id="data_cleanup",
            name="Weekly Data Cleanup",
            max_instances=1,
            replace_existing=True,
        )
        logger.info(f"Cleanup job scheduled: {sc.cleanup_cron}")

    # ---------------------------------------------------------------
    # WHAT HAPPENS WHEN EACH ALARM RINGS
    # ---------------------------------------------------------------

    def _run_etl_job(self):
        """
        Alarm 1 rings → this function runs.
        It processes data (the most important job).
        If it crashes → send a CRITICAL alert immediately.
        """
        from etl.pipeline import ETLPipeline  # import here to keep things simple
        logger.info("=== ETL job starting ===")
        self.monitor.start_run()  # tell monitor we started
        try:
            pipeline = ETLPipeline(self.config, self.monitor)
            pipeline.run()  # actually run the data processing
            self.monitor.finish_run(success=True)  # tell monitor it worked
        except Exception as e:
            # something went wrong
            self.monitor.finish_run(success=False, error=str(e))
            self.alerter.send(
                subject="ETL Pipeline Failed",
                body=str(e),
                severity="CRITICAL",  # most urgent alert level
            )

    def _run_report_job(self):
        """
        Alarm 2 rings → this function runs.
        It generates reports.
        If it crashes → send a WARNING alert (less urgent).
        """
        from report_generation.report_builder import ReportBuilder
        logger.info("=== Report job starting ===")
        try:
            builder = ReportBuilder(self.config, self.monitor)
            builder.generate_all()  # generate all reports
        except Exception as e:
            logger.error(f"Report job failed: {e}", exc_info=True)
            self.alerter.send("Report Generation Failed", str(e), severity="WARNING")

    def _run_cleanup_job(self):
        """
        Alarm 3 rings → this function runs.
        It deletes files older than 30 days to free up space.
        If it crashes → just log it, no alert needed (low priority).
        """
        from utils.cleanup import cleanup_old_files
        logger.info("=== Cleanup job starting ===")
        try:
            cleanup_old_files(self.config, days_to_keep=30)  # delete files older than 30 days
        except Exception as e:
            logger.error(f"Cleanup job failed: {e}", exc_info=True)

    # ---------------------------------------------------------------
    # STARTING AND STOPPING THE ALARM CLOCK
    # ---------------------------------------------------------------

    def start(self):
        """
        Turns the alarm clock ON.
        The program will now keep running and wait for each alarm to ring.
        """
        logger.info("Scheduler starting. Press Ctrl+C to exit.")
        try:
            self.scheduler.start()  # start waiting for alarms
        except (KeyboardInterrupt, SystemExit):
            self.stop()  # if Ctrl+C is pressed, stop cleanly

    def stop(self):
        """
        Turns the alarm clock OFF.
        Shuts everything down cleanly.
        """
        logger.info("Scheduler shutting down...")
        self.scheduler.shutdown(wait=False)  # stop immediately, don't wait

    def _register_signals(self):
        """
        Listens for a "stop" signal from the system or user.
        SIGTERM = system is telling our program to stop (e.g. server shutdown)
        SIGINT  = user pressed Ctrl+C
        """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """
        When a stop signal is received, this runs automatically.
        It stops the scheduler and exits the program.
        """
        logger.info(f"Stop signal received, shutting down...")
        self.stop()
        sys.exit(0)  # exit the program with code 0 (means "exited cleanly")

'''
The main idea to remember is just this:
Create scheduler → Set alarms → Wait → Alarm rings → Run job → Repeat
'''