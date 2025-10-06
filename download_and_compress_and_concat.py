import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import obstore, obstore.store
    import aioftp
    return aioftp, mo


@app.cell
def _(mo):
    mo.md(
        r"""DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`). So we use DWD's FTP server instead."""
    )
    return


@app.cell
def _():
    DWD_FTP_BASE_URL = "opendata.dwd.de"
    DST_BASE_URL = "file:///home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/"
    return (DWD_FTP_BASE_URL,)


@app.cell
async def _(DWD_FTP_BASE_URL, aioftp):
    async with aioftp.Client.context(host=DWD_FTP_BASE_URL) as client:
        for path, info in await client.list("/weather/nwp/icon-eu/grib/00/alb_rad"):
            print(f"{path=}, {info=}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
