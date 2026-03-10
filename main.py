import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

_START_DELAY_HOURS = 2
_RUNTIME = 5


def countdown(hours: int):
    """Eco-safe countdown: minutes only, one log per minute."""
    total_minutes = hours * 60

    logging.info(f"⏳ Delaying start for {total_minutes} minutes")

    while total_minutes > 0:
        hrs = total_minutes // 60
        mins = total_minutes % 60

        logging.info(f"⏳ Starting in {hrs}h {mins}m")

        time.sleep(60)   # sleep one full minute
        total_minutes -= 1


def run_task(task, index, total):
    name = task.__name__
    logging.info(f"▶ [{index}/{total}] Running {name}")

    try:
        task.run()
    except Exception as e:
        logging.exception(f"❌ [{index}/{total}] {name} failed")

    time.sleep(_RUNTIME)


def run_tasks():
    logging.info("🚀 Worker started")

    countdown(_START_DELAY_HOURS)
    logging.info("📅 Starting daily tasks")
    tasks = [all_betcodes, oddslot, get_rightside_odds]

    for i, task in enumerate(tasks, start=1):
        run_task(task, i, len(tasks))

    logging.info("✅ All tasks completed — exiting worker")


if __name__ == "__main__":
    run_tasks()
