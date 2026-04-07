try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
except Exception:  # pragma: no cover
    BackgroundScheduler = None
    IntervalTrigger = None


class _NoopScheduler:
    def start(self) -> None:
        return None

    def shutdown(self, wait: bool = False) -> None:
        return None


def create_scheduler():
    if BackgroundScheduler is None or IntervalTrigger is None:
        return _NoopScheduler()

    def _run_refresh() -> None:
        from backend.web_data_engine.run_pipeline import main as run_opportunity_refresh

        run_opportunity_refresh()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        _run_refresh,
        trigger=IntervalTrigger(hours=24),
        id="pathforge_opportunity_refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    return scheduler
