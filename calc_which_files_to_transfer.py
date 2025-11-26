import marimo

__generated_with = "0.18.1"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import aioftp
    import obstore.store
    from dataclasses import dataclass
    import pathlib
    import re
    import datetime
    from typing import Final, NamedTuple
    return Final, NamedTuple, aioftp, dataclass, datetime, obstore, pathlib, re


@app.cell
def _(PurePosixPath, aioftp):
    type FtpListItem = tuple[PurePosixPath, aioftp.client.BasicListInfo | aioftp.client.UnixListInfo]
    return (FtpListItem,)


@app.cell
async def _(FtpListItem, aioftp):
    async def get_ftp_listing(host: str, path: str) -> list[FtpListItem]:
        """List all files and directories in `path`. If `path` is a file, then result will be empty."""
        # Get the full list of files available on the FTP server for a given NWP init time.
        # This takes about 30 seconds.
        async with aioftp.Client.context(host) as ftp_client:
            ftp_listing = await ftp_client.list(path, recursive=True)
        return ftp_listing


    ftp_listing = await get_ftp_listing("opendata.dwd.de", "/weather/nwp/icon-eu/grib/00/")
    return (ftp_listing,)


@app.cell
def _(ftp_listing):
    ftp_listing[0]
    return


@app.cell
def _(ftp_listing):
    ftp_listing[96]
    return


@app.cell
def _(
    Final,
    FtpListItem,
    aioftp,
    dataclass,
    datetime,
    ftp_listing,
    pathlib,
    re,
):
    @dataclass
    class TransferJob:
        src_ftp_path: pathlib.PurePosixPath
        src_ftp_file_size_bytes: int
        dst_obstore_path: str
        nwp_init_datetime: datetime.datetime


    NWP_INIT_DATETIME_FMT_OBSTORE: Final = "%Y-%m-%dT%HZ"


    def dwd_ftp_path_to_obstore_path(ftp_list_item: FtpListItem) -> None | TransferJob:
        """Converts the FTP path to the destination object store path.

        Or returns None if this file should be ignored.

        This function is designed to work with the style of DWD FTP ICON-EU path in use in 2025, such as:
        /weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2
        """
        ftp_path: pathlib.PurePosixPath = ftp_list_item[0]
        ftp_info: aioftp.client.UnixListInfo = ftp_list_item[1]

        # Skip items that we don't need:
        if ftp_info["type"] == "dir":  # Skip directories.
            return

        if not ftp_path.name.endswith("grib2.bz2"):
            return

        if "pressure-level" in ftp_path.name:  # Skip pressure-level files.
            return

        # Check that any remaining file is the correct shape:
        EXPECTED_N_PARTS: Final = 8
        if len(ftp_path.parts) != EXPECTED_N_PARTS:
            raise ValueError(
                f"Expected the FTP path to have {EXPECTED_N_PARTS}, not {len(ftp_path.parts)}"
            )
        if ftp_path.parts[1:3] != ("weather", "nwp"):
            raise ValueError(
                f"Expected the start of the FTP path to be /weather/nwp/..., not {ftp_path}"
            )

        # Extract the NWP init datetime string from the filename. For example, from this filename:
        #     "...lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2"
        # Extract this:                ^^^^^^^^^^
        nwp_init_date_match = re.search(r"_(20\d{8})_", ftp_path.stem)
        if nwp_init_date_match:
            nwp_init_date_str = nwp_init_date_match.group(1)
        else:
            raise ValueError(f"Failed to match datetime string in {ftp_path.name=}")
        nwp_init_datetime = datetime.datetime.strptime(nwp_init_date_str, "%Y%m%d%H")
        nwp_init_obstore_str = nwp_init_datetime.strftime(NWP_INIT_DATETIME_FMT_OBSTORE)

        # Create dst_obstore_path:
        nwp_model_name = ftp_path.parts[3]
        nwp_variable_name = ftp_path.parts[6]
        dst_obstore_path = f"{nwp_model_name}/regular-lat-lon/{nwp_init_obstore_str}/{nwp_variable_name}/{ftp_path.name}"
        file_size_bytes = int(ftp_info["size"])

        return TransferJob(
            src_ftp_path=ftp_path,
            src_ftp_file_size_bytes=file_size_bytes,
            dst_obstore_path=dst_obstore_path,
            nwp_init_datetime=nwp_init_datetime,
        )


    # Test!
    _ftp_list_item = ftp_listing[100]
    print(f"{_ftp_list_item=}")
    dwd_ftp_path_to_obstore_path(_ftp_list_item)
    return NWP_INIT_DATETIME_FMT_OBSTORE, dwd_ftp_path_to_obstore_path


@app.cell
def _(dwd_ftp_path_to_obstore_path, ftp_listing):
    transfer_jobs = []
    for ftp_list_item in ftp_listing:
        transfer_job = dwd_ftp_path_to_obstore_path(ftp_list_item)
        if transfer_job:
            transfer_jobs.append(transfer_job)
    return (transfer_jobs,)


@app.cell
def _(ftp_listing, transfer_jobs):
    len(ftp_listing), len(transfer_jobs)
    return


@app.cell
def _(transfer_jobs):
    transfer_jobs[0]
    return


@app.cell
def _(transfer_jobs):
    min_nwp_init_datetime = min([transfer_job.nwp_init_datetime for transfer_job in transfer_jobs])
    min_nwp_init_datetime
    return (min_nwp_init_datetime,)


@app.cell
def _(transfer_jobs):
    # Just out of curiosity, let's find the max too:
    max([transfer_job.nwp_init_datetime for transfer_job in transfer_jobs])
    return


@app.cell
def _(NWP_INIT_DATETIME_FMT_OBSTORE: "Final", min_nwp_init_datetime, obstore):
    min_nwp_init_datetime_str = min_nwp_init_datetime.strftime(NWP_INIT_DATETIME_FMT_OBSTORE)

    obstore_listing = (
        obstore.store.LocalStore()
        .list(
            prefix="/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/icon-eu/regular-lat-lon/",
            offset=f"/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/icon-eu/regular-lat-lon/{min_nwp_init_datetime_str}",
        )
        .collect()
    )

    obstore_listing
    return (obstore_listing,)


@app.cell
def _(obstore_listing):
    obstore_listing[0]["path"][68:]
    return


@app.cell
def _(NamedTuple, obstore_listing):
    # Put the obstore items into a set, so we can quickly check for set membership when removing items already downloaded:


    class PathAndSize(NamedTuple):
        """NamedTuple to use as the items in a set."""

        path: str
        file_size_bytes: int


    obstore_set = {PathAndSize(item["path"][68:], item["size"]) for item in obstore_listing}
    obstore_set
    return PathAndSize, obstore_set


@app.cell
def _(PathAndSize, obstore_set, transfer_jobs):
    jobs_still_to_download = []

    for _transfer_job in transfer_jobs:
        ftp_file = PathAndSize(_transfer_job.dst_obstore_path, _transfer_job.src_ftp_file_size_bytes)
        if ftp_file not in obstore_set:
            jobs_still_to_download.append(_transfer_job)
    return (jobs_still_to_download,)


@app.cell
def _(jobs_still_to_download):
    len(jobs_still_to_download)
    return


@app.cell
def _(jobs_still_to_download):
    jobs_still_to_download[0]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
