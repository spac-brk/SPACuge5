import os
import shutil
import requests
import asyncio
import aiohttp # Not used
import pandas as pd


dl_folder = 'Downloads'
status_file = 'download_status.csv'


def download_file(file_name,url):
    status_ = 'Failure'
    try:
        if str(url).startswith('www'):
            url = 'http://' + url
        response = requests.get(url,timeout=5)
        if response.status_code == 200:
            if response.headers['Content-Type'] == 'application/pdf':
                status_ = 'Success'
                notes_ = 'File downloaded'
                with open(dl_folder + '/' + file_name + '.pdf', 'wb') as f:
                    f.write(response.content)
            else:
                notes_ = 'Not a PDF'
        else:
            notes_ = response.reason
    except requests.exceptions.ConnectionError:
        notes_ = "Connection refused"
    except Exception as e:
        notes_ = str(e)
    return status_, notes_


def validate_url(url):
    status_ = 'Failure'
    if str(url) == '':
        notes_ = 'Empty link'
    elif str(url).startswith('file:///'):
        notes_ = 'Local file'
    else:
        status_ = 'Success'
        notes_ = 'URL validated'
    return status_, notes_


async def handle_row(sf,row):
    # Trying first URL
    status, notes = validate_url(row.Pdf_URL)
    if status == 'Success':
        status, notes = download_file(row.BRnum, row.Pdf_URL)

    # Trying second URL
    if status == 'Failure':
        status, notes_alt = validate_url(row.Report_Html_Address)
        if status == 'Success':
            status, notes_alt = download_file(row.BRnum, row.Report_Html_Address)
        notes = notes + ' / ' + notes_alt

    # Write outcome to status file
    if ',' in notes:
        notes = '"' + str(notes).replace('"', '""') + '"'
    sf.write(','.join([row.BRnum, status, notes + '\n']))
    sf.flush()


# https://stackoverflow.com/a/61478547
async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def main():
    # Clear Downloads and status file for testing
    # Comment to continue downloads
    shutil.copyfile('download_status_init.csv', 'download_status.csv')
    shutil.rmtree(dl_folder, ignore_errors=True)

    # Create Downloads folder, if necessary
    if not os.path.exists(dl_folder):
        os.mkdir(dl_folder)

    # Read URLs to be downloaded
    xl_input = pd.read_excel('GRI_2017_2020.xlsx', dtype=str, na_filter=False)

    # Read status on completed download attempts
    status_df = pd.read_csv(status_file).sort_values(by=['BRnum'])

    # Create dataset with items not yet downloaded
    dl_df = xl_input.loc[~xl_input.BRnum.isin(status_df.BRnum), ['BRnum', 'Pdf_URL', 'Report Html Address']]
    dl_df.rename(columns={'Report Html Address': 'Report_Html_Address'}, inplace=True)

    # Create list of tasks and run up to 10 concurrently
    with open(status_file, 'a') as sf:
        tasks = []
        for row in dl_df.itertuples(index=False):
            tasks.append(asyncio.create_task(handle_row(sf, row)))
        await gather_with_concurrency(10, *tasks)


if __name__ == '__main__':
    asyncio.run(main())
    print('Done')
