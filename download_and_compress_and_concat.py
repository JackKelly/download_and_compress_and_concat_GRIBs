import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import obstore, obstore.store
    import aioftp
    from pathlib import Path
    from typing import Optional, NamedTuple
    from collections.abc import Sequence
    import asyncio
    from datetime import datetime, UTC, timedelta
    return (
        NamedTuple,
        Path,
        Sequence,
        UTC,
        aioftp,
        asyncio,
        datetime,
        mo,
        obstore,
        timedelta,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""We use DWD's FTP server instead of DWD's HTTPS server because DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`)."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Concurrent FTP download & `obstore` upload""")
    return


@app.cell
def _(NamedTuple, Path, aioftp, asyncio):
    class FtpTask(NamedTuple):
        src_path: Path
        dst_path: str
        n_retries: int = 0


    class ObstoreTask(NamedTuple):
        data: bytes
        dst_path: str
        n_retries: int = 0


    async def ftp_worker(
        worker_id: int,
        ftp_host: str,
        input_queue: asyncio.Queue[FtpTask],
        output_queue: asyncio.Queue[ObstoreTask],
    ) -> None:
        """A worker that keeps a single FTP connection alive and processes queue items."""
        print(f"Worker {worker_id}: Starting up...")

        # Establish the persistent client connection
        async with aioftp.Client.context(ftp_host) as ftp_client:
            print(f"Worker {worker_id}: Connection established and logged in.")

            # Start continuous processing loop
            while True:
                try:
                    ftp_task: FtpTask = input_queue.get_nowait()
                except asyncio.QueueEmpty:
                    print(f"Worker {worker_id}: Queue of FTP tasks is empty. Finishing.")
                    break

                print(f"Worker {worker_id}: Downloading {ftp_task=}")

                async with ftp_client.download_stream(ftp_task.src_path) as stream:
                    data = await stream.read()

                print(f"Worker {worker_id}: Finished {ftp_task=}")

                await output_queue.put(ObstoreTask(data, ftp_task.dst_path))

                input_queue.task_done()

                # TODO: Handle retries
    return FtpTask, ObstoreTask, ftp_worker


@app.cell
def _(ObstoreTask, asyncio):
    async def obstore_worker(worker_id: int, store, output_queue: asyncio.Queue[ObstoreTask]):
        while True:
            print(f"obstore_worker {worker_id}: Getting output_queue task")
            try:
                _obstore_task: ObstoreTask = await output_queue.get()
            except asyncio.QueueShutDown:
                print(f"obstore_worker {worker_id}: output_queue has shut down!")
                break

            await store.put_async(_obstore_task.dst_path, _obstore_task.data)
            output_queue.task_done()
            print(f"obstore_worker {worker_id}: Done output_queue task")

            # TODO: Handle retries
    return (obstore_worker,)


@app.cell
async def _(FtpTask, Path, asyncio, ftp_worker, obstore, obstore_worker):
    _input_queue = asyncio.Queue(maxsize=10)
    await _input_queue.put(
        FtpTask(
            src_path=Path(
                "/weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025101000_000_ALB_RAD.grib2.bz2"
            ),
            dst_path="/home/jack/data/test.grib2.bz2",
        )
    )

    output_queue = asyncio.Queue(maxsize=10)

    store = obstore.store.LocalStore()

    # --- Start the Pipeline ---
    N_CONCURRENT_FTP_CONNECTIONS = 4
    N_CONCURRENT_OBSTORE_WORKERS = 4
    async with asyncio.TaskGroup() as tg:
        for worker_id in range(N_CONCURRENT_FTP_CONNECTIONS):
            tg.create_task(
                ftp_worker(
                    worker_id=worker_id,
                    ftp_host="opendata.dwd.de",
                    input_queue=_input_queue,
                    output_queue=output_queue,
                )
            )

        for worker_id in range(N_CONCURRENT_OBSTORE_WORKERS):
            tg.create_task(obstore_worker(worker_id, store, output_queue))

        print("await join")
        await _input_queue.join()
        print("joined input_queue!")
        output_queue.shutdown()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Determine which NWP files to transfer""")
    return


@app.cell
def _(Sequence, UTC, datetime, timedelta):
    def get_nwp_init_datetimes(
        nwp_init_hours: Sequence[int], now: datetime = datetime.now(UTC)
    ) -> list[datetime]:
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_midnight = today_midnight - timedelta(days=1)

        nwp_init_datetimes = []
        for nwp_init_hour in nwp_init_hours:
            nwp_init_hour_td = timedelta(hours=nwp_init_hour)
            if nwp_init_hour < now.hour:
                # The NWP was run earlier today.
                nwp_init_datetimes.append(today_midnight + nwp_init_hour_td)
            else:
                # The NWP was run yesterday.
                nwp_init_datetimes.append(yesterday_midnight + nwp_init_hour_td)

        nwp_init_datetimes.sort()
        return nwp_init_datetimes


    NWP_INIT_HOURS = (0, 6, 12, 18)
    nwp_init_datetimes = get_nwp_init_datetimes(nwp_init_hours=NWP_INIT_HOURS)
    nwp_init_datetimes
    return (nwp_init_datetimes,)


@app.cell
def _(Sequence, UTC, datetime, nwp_init_datetimes, timedelta):
    def remove_inconsistent_nwp_model_runs(
        nwp_init_datetimes: Sequence[datetime],
        delay_between_now_and_start_of_update: timedelta,
        duration_of_update: timedelta,
        now: datetime = datetime.now(UTC),
    ) -> list[datetime]:
        """Remove any NWP run whose files are currently being transfered from DWD's HPC
        to DWD's FTP server (and hence is in an inconsistent state on the FTP server).

        For example, for even-numbered ICON-EU initializations, ignore any NWP run if the time now
        is between 2:15 (hh:mm) and 3:45 after the NWP init time.
        For example, ignore the NWP 00Z init if the time now is between 2:15am and 3:45am UTC.

        Warning
        -------
        This function ignores the fact that odd-numbered ICON-EU model runs (3, 9, 15, 21) take less time to transfer from
        DWD's HPC to DWD's FTP server than even-numbered model runs (0, 6, 12, 18). We make this simplifications because we only plan
        to archive even-numbered NWP inits. So this function assumes that NWP model runs have the same delay (which is
        true when we're only considering even-numbered NWP inits).

        Parameters
        ----------
        nwp_init_datetimes : Sequence[datetime]
            The list of datetimes for the NWP initialisations currently available on DWD's FTP server.
        delay_between_now_and_start_of_update : timedelta
            The delay between the time now and when DWD start to copy grib files from DWD's HPC to DWD's FTP server.
        duration_of_update : timedelta
            The duration of the transfer from DWD's HPC to DWD's FTP server.
        now : datetime (defaults to datetime.now(UTC))
        """
        # Ignore NWP inits that are between `ignore_from_dt` and `ignore_to_dt`.
        ignore_to_dt = now - delay_between_now_and_start_of_update
        ignore_from_dt = ignore_to_dt - duration_of_update
        print(f"{now=}\n{ignore_from_dt=}\n{ignore_to_dt=}")
        predicate = lambda init_datetime: not (ignore_from_dt <= init_datetime <= ignore_to_dt)
        return list(filter(predicate, nwp_init_datetimes))


    remove_inconsistent_nwp_model_runs(
        nwp_init_datetimes,
        delay_between_now_and_start_of_update=timedelta(hours=2, minutes=15),
        duration_of_update=timedelta(hours=1, minutes=30),
        now=datetime.now(UTC).replace(hour=2, minute=30),
    )
    return


@app.cell
def _(timedelta):
    timedelta(hours=3, minutes=45) - timedelta(hours=2, minutes=15)
    return


@app.cell
def _():
    5400 / 60
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
