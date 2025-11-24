import marimo

__generated_with = "0.18.0"
app = marimo.App(width="full")


@app.cell
def _():
    import asyncio
    from collections.abc import Sequence
    from datetime import UTC, datetime, timedelta
    from pathlib import Path, PurePosixPath
    from dataclasses import dataclass
    from typing import NamedTuple


    import aioftp
    import arro3
    import marimo as mo
    import obstore
    import obstore.store
    import polars as pl
    return (
        NamedTuple,
        Path,
        PurePosixPath,
        Sequence,
        UTC,
        aioftp,
        arro3,
        asyncio,
        dataclass,
        datetime,
        mo,
        obstore,
        pl,
        timedelta,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We use DWD's FTP server instead of DWD's HTTPS server because DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Concurrent FTP download & `obstore` upload.

    We use an `asyncio.TaskGroup` containing a set of `ftp_worker`s and a set of `obstore_worker`s.

    We use two MPMC queues. Each `ftp_worker` keeps an `aioftp.Client` alive, and repeatedly takes an `FtpTask` off its `input_queue: Queue[FtpTask]`, downloads the FTP file, and puts the binary payload onto its `output_queue: Queue[ObstoreTask]`.
    """)
    return


@app.cell
def _(Path, aioftp, asyncio, dataclass):
    @dataclass
    class FtpTask:
        src_path: Path
        dst_path: str  # The obstore destination.
        n_retries: int = 0


    @dataclass
    class ObstoreTask:
        data: bytes
        dst_path: str
        n_retries: int = 0


    def _log_ftp_exception(ftp_task: FtpTask, e: Exception, ftp_worker_id_str: str) -> None:
        error_str = f"{ftp_worker_id_str} WARNING: "
        # Check if the file is missing from the FTP server:
        if (
            isinstance(e, aioftp.StatusCodeError)
            and isinstance(e.received_codes, tuple)
            and len(e.received_codes) == 1
            and e.received_codes[0].matches("550")
        ):
            error_str += f"File not available on FTP server: {ftp_task.src_path} "

        print(
            error_str, f"ftp_client.download_stream raised exception whilst processing {ftp_task=}", e
        )


    async def ftp_worker(
        worker_id: int,
        ftp_host: str,
        ftp_queue: asyncio.Queue[FtpTask],
        obstore_queue: asyncio.Queue[ObstoreTask],
        failed_ftp_tasks: asyncio.Queue[FtpTask],
        max_retries: int = 3,
    ) -> None:
        """A worker that keeps a single FTP connection alive and processes
        queue items."""
        worker_id_str: str = f"ftp_worker {worker_id}:"
        print(worker_id_str, "Starting up...")

        # Establish the persistent client connection
        async with aioftp.Client.context(ftp_host) as ftp_client:
            print(worker_id_str, "Connection established and logged in.")

            # Start continuous processing loop
            while True:
                try:
                    ftp_task: FtpTask = ftp_queue.get_nowait()
                except asyncio.QueueEmpty:
                    print(worker_id_str, "ftp_queue is empty. Finishing.")
                    break

                print(worker_id_str, f"Attempting to download {ftp_task=}")

                try:
                    async with ftp_client.download_stream(ftp_task.src_path) as stream:
                        data = await stream.read()
                except Exception as e:
                    _log_ftp_exception(ftp_task, e, worker_id_str)
                    if ftp_task.n_retries < max_retries:
                        ftp_task.n_retries += 1
                        print(worker_id_str, "WARNING: Putting ftp_task back on queue to retry later.")
                        await ftp_queue.put(ftp_task)
                    else:
                        print(worker_id_str, "ERROR: Giving up on ftp_task")
                        await failed_ftp_tasks.put(ftp_task)
                else:
                    print(worker_id_str, f"Finished downloading {ftp_task=}")
                    await obstore_queue.put(ObstoreTask(data, ftp_task.dst_path))
                finally:
                    ftp_queue.task_done()
    return FtpTask, ObstoreTask, ftp_worker


@app.cell
def _(ObstoreTask, asyncio, obstore):
    async def obstore_worker(
        worker_id: int, store: obstore.store.ObjectStore, obstore_queue: asyncio.Queue[ObstoreTask]
    ):
        """Obstores are designed to work concurrently, so we can share one `obstore` between tasks."""
        worker_id_str: str = f"obstore_worker {worker_id}:"
        while True:
            print(worker_id_str, "Getting obstore task")
            try:
                obstore_task: ObstoreTask = await obstore_queue.get()
            except asyncio.QueueShutDown:
                print(worker_id_str, "obstore_queue has shut down!")
                break

            await store.put_async(obstore_task.dst_path, obstore_task.data)
            obstore_queue.task_done()
            print(
                worker_id_str,
                f"Done writing to {obstore_task.dst_path} after {obstore_task.n_retries} retries.",
            )

            # TODO: Handle retries
    return (obstore_worker,)


@app.cell
async def _(FtpTask, Path, asyncio, ftp_worker, obstore, obstore_worker):
    _ftp_queue = asyncio.Queue(maxsize=10)
    await _ftp_queue.put(
        FtpTask(
            src_path=Path(
                "/weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025111900_000_ALB_RAD.grib2.bz2"
            ),
            dst_path="/home/jack/data/test.grib2.bz2",
        )
    )

    _failed_ftp_tasks = asyncio.Queue()

    obstore_queue = asyncio.Queue(maxsize=10)

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
                    ftp_queue=_ftp_queue,
                    obstore_queue=obstore_queue,
                    failed_ftp_tasks=_failed_ftp_tasks,
                )
            )

        for worker_id in range(N_CONCURRENT_OBSTORE_WORKERS):
            tg.create_task(obstore_worker(worker_id, store, obstore_queue))

        print("await _ftp_queue.join()")
        await _ftp_queue.join()
        print("joined _ftp_queue!")
        obstore_queue.shutdown()

    if _failed_ftp_tasks.empty():
        print("Good news: No failed FTP tasks!")
    else:
        while True:
            try:
                ftp_task: FtpTask = _failed_ftp_tasks.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                print("Failed FTP task:", ftp_task)
                _failed_ftp_tasks.task_done()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Determine which NWP files to transfer
    """)
    return


@app.cell
def _(Sequence, UTC, datetime, timedelta):
    # TODO: Delete this function. It's not needed if we just grab all the available files on the FTP server.
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

        return nwp_init_datetimes


    NWP_INIT_HOURS = (0, 6, 12, 18)
    nwp_init_datetimes = get_nwp_init_datetimes(nwp_init_hours=NWP_INIT_HOURS)
    nwp_init_datetimes
    return (nwp_init_datetimes,)


@app.cell
def _(Sequence, UTC, datetime, nwp_init_datetimes, timedelta):
    # TODO: Delete this function. It's not needed if we just grab all the available files on the FTP server.
    def remove_inconsistent_nwp_model_runs(
        nwp_init_datetimes: Sequence[datetime],
        delay_between_now_and_start_of_update: timedelta,
        duration_of_update: timedelta,
        now: datetime = datetime.now(UTC),
    ) -> list[datetime]:
        """Remove any NWP run whose files are currently being transfered from
        DWD's HPC to DWD's FTP server (and hence is in an inconsistent state on
        the FTP server).

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


    nwp_init_datetimes_filtered = remove_inconsistent_nwp_model_runs(
        nwp_init_datetimes,
        delay_between_now_and_start_of_update=timedelta(hours=2, minutes=15),
        duration_of_update=timedelta(hours=1, minutes=30),
    )
    nwp_init_datetimes_filtered
    return


@app.cell
def _(Path, UTC, datetime):
    def get_destination_path(base_path: Path, nwp_init_datetime: datetime) -> Path:
        return base_path / nwp_init_datetime.strftime("%Y-%m-%dT%HZ")


    # Test!
    get_destination_path(
        base_path=Path("/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/"),
        nwp_init_datetime=datetime.now(UTC),
    )
    return (get_destination_path,)


@app.cell
def _(
    FtpTask,
    Path,
    Sequence,
    UTC,
    datetime,
    get_destination_path,
    nwp_init_datetimes,
    obstore,
):
    def calculate_which_files_to_transfer(
        ftp_host: str,
        ftp_base_path: Path,
        dst_store: obstore.store.ObjectStore,
        dst_base_path: Path,
        nwp_init_datetimes: Sequence[datetime],
        now: datetime = datetime.now(UTC),
    ) -> list[FtpTask]:
        nwp_init_datetimes.sort()

        # Get listing of all objects in ObjectStore from (and including) the
        # first init datetime available on DWD's FTP server:
        dst_listing = dst_store.list(
            prefix=str(dst_base_path),
            offset=str(get_destination_path(dst_base_path, nwp_init_datetimes[0])),
        )
        print(list(dst_listing))

        # TODO: Use the code below to turn the FTP listing and obstore listings to pl.DataFrames,
        # and then get the set difference (to show which files are available on FTP and not available on S3)
        # and then, for all the files we want to copy from FTP, create the dst_path in the pl.DataFrame.

        # Don't recursively list everything on the FTP server under /weather/nwp/icon-eu/grib/, because we're
        # not interested in the odd-numbered inits. Instead, go init-by-init.

        # Perhaps we don't actually need to filter out the "inconsistent" model runs, now that we're
        # just listing everything. We could make the code simpler _and_ get files with lower latency by just
        # copying everything (and removing remove_inconsistent_nwp_model_runs and get_nwp_init_datetimes).

        # And then test downloading actual data locally.
        # Then think about how to move this code into small-sized PRs for dynamical/reformatters.


    calculate_which_files_to_transfer(
        ftp_host="opendata.dwd.de",
        ftp_base_path=Path("/weather/nwp/icon-eu/grib/"),
        dst_store=obstore.store.LocalStore(),
        dst_base_path=Path(
            "/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/"
        ),
        nwp_init_datetimes=nwp_init_datetimes,
    )
    return


@app.cell
def _(arro3, obstore, pl):
    _store = obstore.store.LocalStore()
    _list = _store.list(
        prefix="/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/2025-10-06T00Z",
        offset="/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/2025-10-06T00Z",
        return_arrow=True,
    ).collect()


    def obstore_listing_to_polars_dataframe(
        record_batch: arro3.core.RecordBatch,
    ) -> pl.DataFrame:
        return (
            pl.DataFrame(record_batch)["path", "size"]
            .with_columns(
                pl.col("path")
                .str.split("/")
                .list.tail(1)
                .list.to_struct(fields=["filename"])
                .struct.unnest()
            )
            .rename({"size": "filesize_bytes"})
        )


    obstore_listing_df = obstore_listing_to_polars_dataframe(_list)
    obstore_listing_df
    return (obstore_listing_df,)


@app.cell
async def _(aioftp):
    # Get the full list of files available on the FTP server for a given NWP init time.
    # This takes about 20 seconds.

    async with aioftp.Client.context("opendata.dwd.de") as ftp_client:
        ftp_listing = await ftp_client.list("/weather/nwp/icon-eu/grib/00/", recursive=True)

    len(ftp_listing)
    return (ftp_listing,)


@app.cell
def _(PurePosixPath, Sequence, ftp_listing):
    type FtpListing = Sequence[tuple[PurePosixPath, dict[str, object]]]


    def filter_ftp_listing(ftp_listing: FtpListing) -> FtpListing:
        def predicate(path_and_meta) -> bool:
            path, _ = path_and_meta
            return "grib2.bz2" in path.name and "pressure-level" not in path.name

        return list(filter(predicate, ftp_listing))


    filtered_ftp_listing = filter_ftp_listing(ftp_listing)
    len(filtered_ftp_listing)
    return FtpListing, filtered_ftp_listing


@app.cell
def _(FtpListing, NamedTuple, PurePosixPath, filtered_ftp_listing, pl):
    class FtpListingRow(NamedTuple):
        path: PurePosixPath
        filename: str
        variable: str
        filesize_bytes: int


    def ftp_listing_to_polars_dataframe(ftp_listing: FtpListing) -> pl.DataFrame:
        flattened_ftp_listing = [
            FtpListingRow(
                path=listing[0],
                filename=listing[0].name,
                variable=listing[0].parts[-2],
                filesize_bytes=listing[1]["size"],
            )
            for listing in ftp_listing
        ]

        return pl.DataFrame(
            flattened_ftp_listing,
            schema=FtpListingRow._fields,
        ).with_columns(
            pl.col("filesize_bytes").str.to_integer(),
        )


    ftp_listing_df = ftp_listing_to_polars_dataframe(filtered_ftp_listing)
    ftp_listing_df
    return (ftp_listing_df,)


@app.cell
def _(ftp_listing_df, obstore_listing_df):
    # Get the set difference between the files on FTP minus the files on object storage

    files_to_copy = ftp_listing_df.join(
        obstore_listing_df, on=["filesize_bytes", "filename"], how="anti"
    )
    files_to_copy
    return (files_to_copy,)


@app.cell
def _(files_to_copy, pl):
    # Compute the dst_path for each file. (First extract the NWP init_datetime from the filename.)

    dst_base_path = (
        "/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10"
    )

    files_to_copy.with_columns(
        init_datetime=(
            pl.col("filename")
            .str.extract(r"_(20\d{8})_")  # extract datetime string like "2025101000"
            .str.pad_end(
                12, "0"
            )  # append two zeros because to_datetime() has to match %H *and* %M. It can't just match just %H.
            .str.to_datetime("%Y%m%d%H%M", time_zone="UTC")
        ),
    ).with_columns(
        dst_path=pl.concat_str(
            [
                pl.lit(dst_base_path),
                pl.col("init_datetime").dt.strftime("%Y-%m-%dT%HZ"),
                pl.col("variable"),
                pl.col("filename"),
            ],
            separator="/",
        ),
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
