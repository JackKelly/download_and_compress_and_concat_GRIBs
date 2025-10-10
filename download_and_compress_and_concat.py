import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import obstore, obstore.store
    import aioftp
    from pathlib import Path, PurePosixPath
    from typing import Optional, NamedTuple
    from collections.abc import Sequence
    import asyncio
    from datetime import datetime, UTC, timedelta
    import polars as pl
    import arro3
    return (
        NamedTuple,
        Path,
        PurePosixPath,
        Sequence,
        UTC,
        aioftp,
        arro3,
        asyncio,
        datetime,
        mo,
        obstore,
        pl,
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
    return


app._unparsable_cell(
    r"""
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

        # TODO: Use the code below to turn the FTP listing and obstore listings to DataFrames,
        # and then get the set difference (to show which files are available on FTP and not available on S3)
        # and then, for all the files we want to copy from FTP, create the dst_path in the pl.DataFrame.

        # Don't recursively list everything on the FTP server under /weather/nwp/icon-eu/grib/, because we're
        # not interested in the odd-numbered inits or the init we're gonna leave out because it's inconsistent.
        # Although, perhaps we don't actually need to filter out the \"inconsistent\" model runs, now that we're 
        # just listing everything. We could make the code simpler _and_ get files with lower latency by just
        # copying everything.

        # And then test downloading actual data locally.
        # Then think about how to move this code into small-sized PRs for dynamical/reformatters.

        )


    calculate_which_files_to_transfer(
        ftp_host=\"opendata.dwd.de\",
        ftp_base_path=Path(\"/weather/nwp/icon-eu/grib/\"),
        dst_store=obstore.store.LocalStore(),
        dst_base_path=Path(
            \"/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/\"
        ),
        nwp_init_datetimes=nwp_init_datetimes_filtered,
    )
    """,
    name="_"
)


@app.cell
def _(arro3, obstore, pl):
    _store = obstore.store.LocalStore()
    _list = _store.list(
        prefix="/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/2025-10-06T00Z",
        offset="/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/testing-2025-10-10/2025-10-06T00Z",
        return_arrow=True,
    ).collect()


    def obstore_listing_to_polars_dataframe(record_batch: arro3.core.RecordBatch) -> pl.DataFrame:
        return pl.DataFrame(record_batch)["path", "size"].with_columns(
            pl.col("path")
            .str.split("/")
            .list.tail(1)
            .list.to_struct(fields=["filename"])
            .struct.unnest()
        )


    obstore_listing_df = obstore_listing_to_polars_dataframe(_list)
    obstore_listing_df
    return (obstore_listing_df,)


@app.cell
async def _(aioftp):
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
        size: int


    def ftp_listing_to_polars_dataframe(ftp_listing: FtpListing) -> pl.DataFrame:
        flattened_ftp_listing = [
            FtpListingRow(
                path=listing[0],
                filename=listing[0].name,
                variable=listing[0].parts[-2],
                size=listing[1]["size"],
            )
            for listing in ftp_listing
        ]

        return pl.DataFrame(
            flattened_ftp_listing,
            schema=FtpListingRow._fields,
        ).with_columns(
            pl.col("size").str.to_integer(),
        )


    ftp_listing_df = ftp_listing_to_polars_dataframe(filtered_ftp_listing)
    ftp_listing_df
    return (ftp_listing_df,)


@app.cell
def _(ftp_listing_df, obstore_listing_df):
    # Get the set difference between the files on FTP minus the files on object storage

    files_to_copy = ftp_listing_df.join(obstore_listing_df, on=["size", "filename"], how="anti")
    files_to_copy
    return (files_to_copy,)


@app.cell
def _(files_to_copy, pl):
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
