import pandas as pd
import datetime


class MedicalDevices:
    """Класс для работы с таблицей медицинских устройств."""

    def __init__(self, filename: str):
        """Инициализатор класса.

        Args:
            filename: Название файла с таблицей, которую мы читаем.
        """

        self.filename = filename
        self.df = pd.read_excel(f'{self.filename}.xlsx')
        self.time = pd.Timestamp.today().strftime('%Y-%m-%d')
        self.date_format = '%Y-%m-%d'

    def _get_correct_format(self, data_frame):
        """Метод для перевода дат в удобный для работы формат.

        Args:
            data_frame: Столбец данных, для которых требуется изменение формата дат.
        """

        correct_datas = []

        for i in range(len(data_frame)):
            if type(data_frame[i]) != str:
                correct_datas.append('NULL')
            elif '.' in data_frame[i]:
                correct_datas.append(datetime.datetime.strptime(
                    data_frame[i], '%d.%m.%Y').date().strftime(self.date_format))
            elif '-' in data_frame[i]:
                correct_datas.append(datetime.datetime.strptime(
                    data_frame[i], '%Y-%m-%d').date().strftime(self.date_format))
            elif ',' in data_frame[i]:
                correct_datas.append(datetime.datetime.strptime(
                    data_frame[i], '%b %d, %Y').date().strftime(self.date_format))
            else:
                correct_datas.append('NULL')

        return correct_datas

    def correct_dates(self, columns: list[str]):
        """Метод для применения перевода дат из столбцов в удобный формат.

        Args:
            columns: Названия столбцов.
        """

        for column in columns:
            self.df[column] = self._get_correct_format(self.df[column])

        return self.df

    def correct_status(self, status_column: str):
        """Метод для приведения статусов устройств к единому формату.

        Args:
            status_column: Название столбца со статусами устройств.
        """

        statuses = self.df[status_column]

        for i in range(len(statuses)):
            if statuses[i].lower() in ['ok', 'op', 'operational', 'working']:
                statuses[i] = 'operational'
            elif statuses[i].lower() in ['broken', 'error', 'needs_repair']:
                statuses[i] = 'faulty'
            elif statuses[i].lower() in ['maintenance', 'maint_sched', 'service_scheduled']:
                statuses[i] = 'maintenance_scheduled'
            elif statuses[i].lower() in ['planned', 'to_install', 'scheduled_install']:
                statuses[i] = 'planned_installation'

        self.df[status_column] = statuses

    def _calibrations_results(self, data_frame: pd.DataFrame):
        """Метод для определения статуса калибровки устройства.

        Args:
            data_frame: Строка DataFrame с данными об устройстве.
        """

        if data_frame['last_calibration_date'] == 'NULL':
            return 'NO_CALIBRATION'

        elif data_frame['last_calibration_date'] < data_frame['install_date']:
            return 'INCORRECT'

        elif data_frame['last_calibration_date'] < self.time:
            return 'EXPIRED'

        elif data_frame['last_calibration_date'] >= self.time:
            return 'OK'

    def calibrations_sheet(self):
        """Метод для создания отчета по калибровкам устройств.

        Returns:
            DataFrame с информацией о калибровках и статусом каждой.
        """

        calibrations = self.df[['device_id', 'clinic_id', 'clinic_name',
                                'install_date', 'last_calibration_date']]

        calibrations['Report'] = calibrations.apply(self._calibrations_results, axis=1)

        return calibrations

    def get_summary_table(self):
        """Метод для формирования сводной таблицы по клиникам.

        Returns:
            DataFrame со сводной информацией: количество устройств,
            количество проблем, количество отказов, среднее время работы.
        """

        summary_table = self.df.groupby(
            ["clinic_id", "clinic_name"], as_index=False).agg(
            device_count=('device_id', 'count'),
            issues_reported_12mo=('issues_reported_12mo', 'sum'),
            failure_count_12mo=('failure_count_12mo', 'sum'),
            avg_uptime=('uptime_pct', 'mean')
        )

        summary_table['avg_uptime'] = summary_table['avg_uptime'].round(2)

        return summary_table