import time
import logging
import all_betcodes
import get_rightside_odds
import oddslot

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Constants
_START_DELAY_HOURS = 4   # initial delay in hours
_RUNTIME = 5             # seconds between each task


def countdown(hours: int):
    total_seconds = hours * 3600
    while total_seconds > 0:
        hrs, remainder = divmod(total_seconds, 3600)
        mins, secs = divmod(remainder, 60)
        logging.info(f"⏳ Starting in {hrs}h {mins}m {secs}s")
        time.sleep(60)  # update every minute
        total_seconds -= 60

def run_task(task, index, total):
    """Run a single task with logging and sleep delay."""
    task_name = task.__name__
    message = f"▶ [{index}/{total}] Running {task_name}..."
    logging.info(message)
    print(message)
    try:
        task.run()
    except Exception as e:
        error_msg = f"❌ [{index}/{total}] {task_name} failed: {e}"
        logging.error(error_msg)
        print(error_msg)
    time.sleep(_RUNTIME)


def run_tasks():
    # Countdown before start
    start_message = f"⏳ Waiting for {_START_DELAY_HOURS} hours before starting tasks..."
    logging.info(start_message)
    print(start_message)
    countdown(_START_DELAY_HOURS)

    logging.info("📅 Starting daily tasks...")
    print("📅 Starting daily tasks...")

    tasks = [all_betcodes, oddslot, get_rightside_odds]
    total_tasks = len(tasks)

    for idx, task in enumerate(tasks, start=1):
        run_task(task, idx, total_tasks)

    success_message = "✅ All tasks completed successfully."
    logging.info(success_message)
    print(success_message)


if __name__ == "__main__":
    run_tasks()
