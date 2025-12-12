import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import aioftp
    import obstore.store
    from dataclasses import dataclass
    import pathlib
    from pathlib import PurePosixPath
    import re
    import datetime
    from typing import Final, NamedTuple
    from abc import ABC, abstractmethod
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from obstore import ObjectMeta
    return (
        ABC,
        Final,
        NamedTuple,
        PurePosixPath,
        abstractmethod,
        aioftp,
        dataclass,
        datetime,
        obstore,
        re,
    )


@app.cell
def _(PurePosixPath, aioftp):
    type FtpListItem = tuple[PurePosixPath, aioftp.client.BasicListInfo | aioftp.client.UnixListInfo]
    return (FtpListItem,)


@app.cell
async def _(
    ABC,
    Final,
    FtpListItem,
    NamedTuple,
    PurePosixPath,
    abstractmethod,
    aioftp,
    dataclass,
    datetime,
    obstore,
    re,
):
    @dataclass
    class TransferJob:
        src_ftp_path: PurePosixPath
        src_ftp_file_size_bytes: int

        # `dst_obstore_path` starts at the datetime part of the path, for example `dst_obstore_path` could be:
        # '2025-12-12T00Z/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025121200_000_ALB_RAD.grib2.bz2'
        dst_obstore_path: PurePosixPath

        # Extracted from the FTP path
        nwp_init_datetime: datetime.datetime


    # Put the obstore items into a set, so we can quickly check for set membership when removing items already downloaded:
    class PathAndSize(NamedTuple):
        """NamedTuple to use as the items in a set."""

        path: str
        file_size_bytes: int


    class FtpTransferCalculator(ABC):
        async def transfer_new_files_for_all_nwp_inits(self):
            ftp_paths_for_nwp_inits: list[
                PurePosixPath
            ] = await self.ftp_path_for_every_nwp_init_available_on_ftp_server()

            # TODO(Jack): Filter paths! We only want the 00, 06, 12, 18 inits!

            for ftp_path in ftp_paths_for_nwp_inits:
                await self.transfer_new_files_for_single_nwp_init(ftp_path)

        async def transfer_new_files_for_single_nwp_init(
            self, ftp_path_of_nwp_init: PurePosixPath
        ) -> None:
            ftp_listing = await self.ftp_listing_for_nwp_init(ftp_path_of_nwp_init)

            # Collect list of TransferJobs from FTP server's listing:
            ftp_transfer_jobs: list[TransferJob] = []
            for ftp_list_item in ftp_listing:
                if not self.skip_ftp_item(ftp_list_item):
                    transfer_job = self.ftp_list_item_to_transfer_job(ftp_list_item)
                    ftp_transfer_jobs.append(transfer_job)

            # Find the earliest NWP init datetime.
            # We'll use this as the `offset` when listing objects on object storage.
            min_nwp_init_datetime = min(
                [transfer_job.nwp_init_datetime for transfer_job in ftp_transfer_jobs]
            )

            obstore_listing_set = self.obstore_listing_for_nwp_init(min_nwp_init_datetime)

            print("example entry in obstore_listing_set:", list(obstore_listing_set)[:5])

            jobs_still_to_download: list[TransferJob] = []

            for transfer_job in ftp_transfer_jobs:
                full_obstore_path = str(self.obstore_root_path / transfer_job.dst_obstore_path)
                ftp_file = PathAndSize(full_obstore_path, transfer_job.src_ftp_file_size_bytes)
                if ftp_file not in obstore_listing_set:
                    jobs_still_to_download.append(transfer_job)
                print("example full_obstore_path: ", full_obstore_path)
                break

            # TODO(Jack): Send `jobs_still_to_download` to FTP download client
            return jobs_still_to_download

        @property
        @abstractmethod
        def obstore_root_path(self) -> PurePosixPath:
            pass

        @abstractmethod
        def obstore_listing_for_nwp_init(self, nwp_init: datetime.datetime) -> set[PathAndSize]:
            pass

        @abstractmethod
        async def ftp_listing_for_nwp_init(self, path: str) -> list[FtpListItem]:
            pass

        @abstractmethod
        async def ftp_path_for_every_nwp_init_available_on_ftp_server(
            self,
        ) -> list[PurePosixPath]:
            pass

        def skip_ftp_item(self, ftp_list_item: FtpListItem) -> bool:
            """Skip FTP items that we don't need."""
            ftp_path: PurePosixPath = ftp_list_item[0]
            ftp_info: aioftp.client.UnixListInfo = ftp_list_item[1]

            if ftp_info["type"] == "dir":  # Skip directories.
                return True

            if not ftp_path.name.endswith("grib2.bz2"):
                return True

            if "pressure-level" in ftp_path.name:  # Skip pressure-level files.
                return True

            return False

        @abstractmethod
        def sanity_check_ftp_path(self, ftp_path: PurePosixPath) -> None:
            pass

        @abstractmethod
        def extract_init_datetime_from_ftp_path(self, ftp_path: PurePosixPath) -> datetime.datetime:
            pass

        @abstractmethod
        def extract_nwp_variable_name_from_ftp_path(self, ftp_path: PurePosixPath) -> str:
            pass

        @property
        def format_string_for_nwp_init_datetime_in_obstore_path(self) -> str:
            return "%Y-%m-%dT%HZ"

        def ftp_list_item_to_transfer_job(self, ftp_list_item: FtpListItem) -> TransferJob:
            """Converts the FTP path to the destination object store path.

            This function is designed to work with the style of DWD FTP ICON-EU path in use in 2025, such as:
            /weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2
            """
            ftp_path: PurePosixPath = ftp_list_item[0]
            ftp_info: aioftp.client.UnixListInfo = ftp_list_item[1]

            self.sanity_check_ftp_path(ftp_path)

            nwp_init_datetime: datetime.datetime = self.extract_init_datetime_from_ftp_path(ftp_path)
            nwp_init_datetime_obstore_str = nwp_init_datetime.strftime(
                self.format_string_for_nwp_init_datetime_in_obstore_path
            )

            # Create dst_obstore_path:
            nwp_variable_name = self.extract_nwp_variable_name_from_ftp_path(ftp_path)
            dst_obstore_path = (
                PurePosixPath(nwp_init_datetime_obstore_str) / nwp_variable_name / ftp_path.name
            )
            file_size_bytes = int(ftp_info["size"])

            return TransferJob(
                src_ftp_path=ftp_path,
                src_ftp_file_size_bytes=file_size_bytes,
                dst_obstore_path=dst_obstore_path,
                nwp_init_datetime=nwp_init_datetime,
            )


    class DwdFtpTransferCalculator(FtpTransferCalculator):
        @property
        def obstore_root_path(self) -> PurePosixPath:
            return PurePosixPath(
                "home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/icon-eu/regular-lat-lon/"
            )

        async def ftp_listing_for_nwp_init(self, path: str) -> list[FtpListItem]:
            """List all files and directories in `path`. If `path` is a file, then result will be empty."""
            # Get the full list of files available on the FTP server for a given NWP init time.
            # This takes about 30 seconds.
            async with aioftp.Client.context("opendata.dwd.de") as ftp_client:
                ftp_listing = await ftp_client.list(path, recursive=True)
            return ftp_listing

        async def ftp_path_for_every_nwp_init_available_on_ftp_server(
            self,
        ) -> list[PurePosixPath]:
            async with aioftp.Client.context("opendata.dwd.de") as ftp_client:
                ftp_listing: list[FtpListItem] = await ftp_client.list("/weather/nwp/icon-eu/grib")
            paths: list[PurePosixPath] = []
            for ftp_list_item in ftp_listing:
                ftp_path: PurePosixPath = ftp_list_item[0]
                ftp_info: aioftp.client.UnixListInfo = ftp_list_item[1]
                if ftp_info["type"] == "dir":
                    paths.append(ftp_path)
            return paths

        def sanity_check_ftp_path(self, ftp_path: PurePosixPath) -> None:
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

        def extract_init_datetime_from_ftp_path(self, ftp_path: PurePosixPath) -> datetime.datetime:
            # Extract the NWP init datetime string from the filename. For example, from this filename:
            #     "...lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2"
            # Extract this:                ^^^^^^^^^^
            nwp_init_date_match = re.search(r"_(20\d{8})_", ftp_path.stem)
            if nwp_init_date_match:
                nwp_init_date_str = nwp_init_date_match.group(1)
            else:
                raise ValueError(f"Failed to match datetime string in {ftp_path.name=}")
            return datetime.datetime.strptime(nwp_init_date_str, "%Y%m%d%H")

        def extract_nwp_variable_name_from_ftp_path(self, ftp_path: PurePosixPath) -> str:
            return ftp_path.parts[6]

        # TOOD(Jack): This is just for testing locally!
        def obstore_listing_for_nwp_init(self, nwp_init: datetime.datetime) -> set[PathAndSize]:
            nwp_init_datetime_str = nwp_init.strftime(
                self.format_string_for_nwp_init_datetime_in_obstore_path
            )

            obstore_listing = (
                obstore.store.LocalStore()
                .list(
                    prefix=f"{self.obstore_root_path}",
                    offset=f"{self.obstore_root_path / nwp_init_datetime_str}",
                )
                .collect()
            )

            return {PathAndSize(item["path"], item["size"]) for item in obstore_listing}


    # Test!
    ftp_calc = DwdFtpTransferCalculator()
    ftp_paths_for_nwp_inits: list[
        PurePosixPath
    ] = await ftp_calc.ftp_path_for_every_nwp_init_available_on_ftp_server()
    ftp_paths_for_nwp_inits
    return ftp_calc, ftp_paths_for_nwp_inits


@app.cell
async def _(ftp_calc, ftp_paths_for_nwp_inits: "list[PurePosixPath]"):
    jobs_still_to_transfer = await ftp_calc.transfer_new_files_for_single_nwp_init(
        ftp_paths_for_nwp_inits[0]
    )
    return (jobs_still_to_transfer,)


@app.cell
def _(jobs_still_to_transfer):
    jobs_still_to_transfer[0]
    return


@app.cell
def _(jobs_still_to_transfer):
    len(jobs_still_to_transfer)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
