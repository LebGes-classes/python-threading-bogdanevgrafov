import asyncio
import pandas as pd
import time
import threading
from Medical_Devices import (
    MedicalDevices,
)


def process_file_sync(filename: str):
    """Синхронная обработка одного файла.

    Args:
        filename - Название файла
    """

    worker = MedicalDevices(filename)

    # Correct form
    worker.correct_dates([
        'install_date',
        'warranty_until',
        'last_calibration_date',
        'last_service_date'
    ])
    worker.correct_status('status')

    # Warranty
    sorted_warranty = worker.df.sort_values(['warranty_until'])

    actual_warranties = sorted_warranty[
        sorted_warranty['warranty_until'] >= worker.time]

    non_actual_warranties = sorted_warranty[
        sorted_warranty['warranty_until'] < worker.time]

    # Problems
    problems = worker.df.groupby(
        ["clinic_id", "clinic_name"], as_index=False).agg(
        issues_reported_12mo=('issues_reported_12mo', 'sum')
    )

    problems = problems.sort_values(
        ['issues_reported_12mo'], ascending=False)

    # Calibration
    calibration_report = worker.calibrations_sheet()

    # Summary Table
    summary_table = worker.get_summary_table()

    # Запись в файл
    with pd.ExcelWriter(f'{filename}_report.xlsx') as writer:
        worker.df.to_excel(
            writer, index=False, sheet_name='Medical_Devices')
        actual_warranties.to_excel(
            writer, index=False, sheet_name='Actual_Warranties')
        non_actual_warranties.to_excel(
            writer, index=False, sheet_name='Non-Actual_Warranties')
        problems.to_excel(
            writer, index=False, sheet_name='Problems')
        calibration_report.to_excel(
            writer, index=False, sheet_name='Calibration_Report')
        summary_table.to_excel(
            writer, index=False, sheet_name='Summary_Table')


async def process_file(filename: str):
    """Асинхронная обработка одного файла.

    Args:
        filename - Название файла
    """

    process_file_sync(filename)


async def async_main():
    """Асинхронный запуск обработки файлов."""

    tasks = []
    for i in range(1, 11):
        tasks.append(process_file(
            f'/Users/batya/myworks/pandos/files/async_data/medical_diagnostic_devices_{i}'
        ))

    await asyncio.gather(*tasks)


def threading_main():
    """Многопоточный запуск обработки файлов."""

    threads = []

    for i in range(1, 11):
        filename = f'/Users/batya/myworks/pandos/files/async_data/medical_diagnostic_devices_{i}'

        t = threading.Thread(target=process_file_sync, args=(filename,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


def sync_main():
    """Синхронный запуск обработки файлов."""

    files = [
        f'/Users/batya/myworks/pandos/files/async_data/medical_diagnostic_devices_{i}'
        for i in range(1, 11)
    ]

    for file in files:
        process_file_sync(file)


if __name__ == "__main__":

    # Синхронное выполнение
    start = time.time()
    sync_main()
    sync_time = time.time() - start
    print(f"Sync time: {sync_time:.3f} seconds")

    # Асинхронное выполнение
    start = time.time()
    asyncio.run(async_main())
    async_time = time.time() - start
    print(f"Async time: {async_time:.3f} seconds")

    # Многопоточное выполнение
    start = time.time()
    threading_main()
    thread_time = time.time() - start
    print(f"Threading time: {thread_time:.3f} seconds")