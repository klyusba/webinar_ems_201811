from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from itertools import product
import requests
import lxml.html as html
import xlrd
import zipfile
import time
import io
import os
import pandas as pd


# TODO use _cell_values for best performance


ATS_REPORTS_LIST_URL = 'https://www.atsenergo.ru/nreport?access=public&rname={report}&rdate={date:%Y%m%d}&region={pz}'
ATS_REPORT_URL = 'https://www.atsenergo.ru/nreport'
SO_REPORT_URL = 'http://br.so-ups.ru/Public/Export/Csv/{report}.aspx?'
price_zones = [None, 'eur', 'sib']

regions = pd.read_csv(os.path.join(os.path.dirname(__file__), 'regions.csv'),
                      sep=';',
                      encoding='cp1251',
                      index_col='region_pk')

region_map = {
    region_name: i
    for i, region_name in zip(regions.index, regions.region_name)
}

null = float('NaN')


session = requests.session()
def try_urlopen(url: str):
    i = 0
    while i < 5:
        try:
            page = session.get(url)
            break
        except Exception as e:
            print(str(e))
            print('Жду 1 секунду и пробую снова')
            time.sleep(1)
            i += 1
    else:
        raise ValueError('Не удалось скачать данные')

    return io.BytesIO(page.content)


class Report:
    report_type = None
    data = None

    def download(self, *args, **kwargs):
        raise NotImplementedError()


class AtsReport(Report):
    _context = None
    pz = [1, 2]
    period = 'day'

    @staticmethod
    def _get_reports(report_type: str, target_date: date, pz: int):
        if report_type is None or date is None or pz is None:
            raise ValueError('Все атрибуты должны быть заполнены')
        url = ATS_REPORTS_LIST_URL.format(report=report_type, date=target_date, pz=price_zones[pz])

        page = html.parse(try_urlopen(url)).getroot()
        url_list = [ATS_REPORT_URL + a.get('href') for a in page.xpath('.//a[contains(@href, "zip=1")]')]
        return url_list

    @staticmethod
    def __get_xls(url):
        content = try_urlopen(url)
        with zipfile.ZipFile(content) as zip:
            fn = zip.namelist()[0]
            with zip.open(fn) as xls:
                return xlrd.open_workbook(file_contents=xls.read())

    def _read_xls(self, wb: xlrd.book.Book):
        raise NotImplementedError()

    def _download(self, urls):
        for url in urls:
            wb = self.__get_xls(url)
            self._read_xls(wb)

    def download(self, start_date, end_date=None):
        if end_date is None:
            periods = [start_date, ]
        elif self.period == 'day':
            day_count = (end_date - start_date).days + 1
            periods = [start_date + timedelta(n) for n in range(day_count)]
        elif self.period == 'month':
            rd = relativedelta(end_date, start_date)
            month_count = rd.years * 12 + rd.months + 1
            periods = [start_date + relativedelta(months=n) for n in range(month_count)]

        self.data = []
        for d, pz in product(periods, self.pz):
            self._context = d, pz
            self._download(url for url in self._get_reports(self.report_type, d, pz))

        return pd.concat(self.data)  # type: pd.DataFrame


class BranchReport(AtsReport):
    report_type = 'TS_PART_REP_LINE'

    def _read_xls(self, wb: xlrd.book.Book):
        data = list()
        for ws in wb.sheets():  # type: xlrd.sheet.Sheet
            hour_data = pd.DataFrame(
                data={
                    'hour': int(ws.name),
                    'node_from': ws.col_values(0, 5),
                    'node_to': ws.col_values(1, 5),
                    'branch_num': ws.col_values(2, 5),
                    'flow': ws.col_values(3, 5)
                }
            )
            data.append(hour_data)

        data = pd.concat(data)  # type: pd.DataFrame
        data['date'] = pd.to_datetime(self._context[0])
        # в какой-то момент int превращаются в float
        data.node_from = data.node_from.astype(int)
        data.node_to = data.node_to.astype(int)
        data.branch_num = data.branch_num.astype(int)
        data.hour = data.hour.astype(int)

        self.data.append(data)


class NodePriceReport(AtsReport):
    report_type = 'big_nodes_prices_pub'

    def _read_xls(self, wb: xlrd.book.Book):
        data = list()
        for ws in wb.sheets():  # type: xlrd.sheet.Sheet
            hour_data = pd.DataFrame(
                data=ws._cell_values[3:],
                columns=[
                    'node_id',
                    'node_name',
                    'u',
                    'region_id',
                    'price',
                    'empty'
                ]
            )
            hour_data.drop(['node_name', 'u', 'empty'], axis=1, inplace=True)
            hour_data['hour'] = int(ws.name)
            hour_data['region_id'] = hour_data.region_id.replace(region_map).astype(int)
            hour_data['price'] = pd.to_numeric(hour_data.price)
            data.append(hour_data)

        data = pd.concat(data)  # type: pd.DataFrame
        data['date'] = pd.to_datetime(self._context[0])
        # в какой-то момент int превращаются в float
        data.node_id = data.node_id.astype(int)
        data.hour = data.hour.astype(int)

        self.data.append(data)


class DguVolumeReport(AtsReport):
    report_type = 'carana_sell_units'

    def _read_xls(self, wb: xlrd.book.Book):
        data = list()
        ws = wb.sheet_by_index(0)  # type: xlrd.sheet.Sheet
        row = 7
        while ws.cell_value(row, 0) != '':
            dgu_id = int(ws.cell_value(row, 0))
            node_id = int(ws.cell_value(row, 2))
            data_row = ws.row_values(row, 4, 124)
            for i in range(0, 5*24, 5):
                data.append((dgu_id, i // 5, node_id, data_row[i], data_row[i + 1], data_row[i + 2], data_row[i + 3], data_row[i + 4]))
            row += 1

        data = pd.DataFrame(data, columns=['dgu_id', 'hour', 'node_id', 'p_min_tech', 'p_min_techn', 'p_min', 'v_ppp', 'p_max'])
        data['date'] = pd.to_datetime(self._context[0])
        # в какой-то момент int превращаются в float
        data.node_id = data.node_id.astype(int)
        data.hour = data.hour.astype(int)
        data.dgu_id = data.dgu_id.astype(int)

        self.data.append(data)


class DemandOfferCurveReport(AtsReport):
    report_type = 'curve_demand_offer'

    def _read_xls(self, wb: xlrd.book.Book):
        data = list()
        for ws in wb.sheets():  # type: xlrd.sheet.Sheet
            p_list = ws.col_values(3, 8)
            if p_list[0] == "*":
                p_list[0] = 0

            hour_data = pd.DataFrame(
                data={
                    'hour': int(ws.name),
                    'price': p_list,
                    'volume': ws.col_values(4, 8)
                }
            )
            data.append(hour_data)

        data = pd.concat(data)  # type: pd.DataFrame
        date, pz = self._context
        data['date'] = pd.to_datetime(date)
        data['pz'] = pz
        # в какой-то момент int превращаются в float
        data.pz = data.pz.astype(int)
        data.hour = data.hour.astype(int)

        self.data.append(data)


class SectionReport(AtsReport):
    report_type = 'overflow_sechen_all_pub'

    def _read_xls(self, wb: xlrd.book.Book):
        data = list()
        for ws in wb.sheets():  # type: xlrd.sheet.Sheet
            hour_data = pd.DataFrame(
                data=ws._cell_values[3:],
                columns=[
                    'section_id',
                    'name',
                    'node1',
                    'node2',
                    'branch_name',
                    'p_min',
                    'p_max',
                    'v_ppp'
                ]
            )
            hour_data.replace(['', ' '], [null, null], inplace=True)
            t = hour_data.p_min.isnull() & hour_data.p_max.isnull()
            hour_data.drop(hour_data.index.values[t], inplace=True)
            hour_data.p_min = hour_data.p_min.astype(str).str.replace(',', '.').astype('float')
            hour_data.p_max = hour_data.p_max.astype(str).str.replace(',', '.').astype('float')
            hour_data.v_ppp = hour_data.v_ppp.astype(str).str.replace(',', '.').astype('float')
            hour_data['is_active'] = ((hour_data.p_min == hour_data.v_ppp) | (hour_data.p_max == hour_data.v_ppp)) * 1
            hour_data.drop(['name', 'node1', 'node2', 'branch_name', 'p_min', 'p_max', 'v_ppp'], axis=1, inplace=True)
            hour_data['hour'] = int(ws.name)
            data.append(hour_data)

        data = pd.concat(data)  # type: pd.DataFrame
        data['date'] = pd.to_datetime(self._context[0])
        # в какой-то момент int превращаются в float
        data.section_id = data.section_id.astype(int)
        data.hour = data.hour.astype(int)
        data.is_active = data.is_active.astype(int)

        self.data.append(data)


class RegionReport(AtsReport):
    report_type = 'trade_region_spub'
    pz = [1, ]  # данные для обоих ЦЗ дублируются

    def _read_xls(self, wb: xlrd.book.Book):
        ws = wb.sheet_by_index(0)
        data = pd.DataFrame(
            data=ws._cell_values[6:],
            columns=[
                'region_id',
                'hour',
                'gen_ges',
                'gen_aes',
                'gen_tes',
                'gen_ses',
                'gen_ves',
                'gen_other',
                'pmin_tech_ges',
                'pmin_tech_aes',
                'pmin_tech_tes',
                'pmin_tech_ses',
                'pmin_tech_ves',
                'pmin_tech_other',
                'pmin_ges',
                'pmin_aes',
                'pmin_tes',
                'pmin_ses',
                'pmin_ves',
                'pmin_other',
                'pmax_ges',
                'pmax_aes',
                'pmax_tes',
                'pmax_ses',
                'pmax_ves',
                'pmax_other',
                'con',
                'exp',
                'imp',
                'price_con',
                'price_gen'
            ]
        )
        data.replace('', null, inplace=True)
        data['region_id'] = data.region_id.replace(region_map).astype(int)
        data['hour'] = data.hour.astype(int)
        data['date'] = pd.to_datetime(self._context[0])
        self.data.append(data)


class RegionFactReport(AtsReport):
    """Отчет по фактическому потреблению в регионах"""
    report_type = 'fact_region'
    pz = [1, ]  # данные для обоих ЦЗ дублируются
    period = 'month'
    
    def _read_xls(self, wb: xlrd.book.Book):
        ws = wb.sheet_by_index(0)
        data = pd.DataFrame(
            data=ws._cell_values[7:],
            columns=['date', 'hour', 'fact']
        )
        data['region_id'] = region_map[ws._cell_values[1][1]]
        data['date'] = pd.to_datetime(data['date'])
        data['hour'] = data.hour.astype(int) - 1
        self.data.append(data)


class RegionTotalReport(Report):
    """Составление отчета по всем доступным данным в разрезе региона.
    Комбинация отчетов:
        Отчет о торгах по субъектам РФ ЕЭС
        Отчёт о перетоках мощности по контролируемым сечениям
    """

    def download(self, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        day_count = (end_date - start_date).days + 1
        days = [start_date + timedelta(n) for n in range(day_count)]
        region_report = RegionReport()
        section_report = SectionReport()
        blk_report = SOBlockStationsReport()
        
        total_report = []
        for day in days:
            region_data = region_report.download(day)
            section_data = section_report.download(day)
            blck_stan_data = blk_report.download(day)
            
            # в данных по сечениям есть повторы
            section_data = section_data.groupby(['date', 'hour', 'section_id'])['is_active'].max().reset_index()
            active_sections = section_data.groupby('section_id')['is_active'].sum()
            active_sections = active_sections[active_sections > 0].index
            section_data = section_data[section_data.section_id.isin(active_sections)]
            section_data = section_data.set_index(['date', 'hour', 'section_id']).unstack(level='section_id')
            section_data.columns = ['{}_{}'.format(*c) for c in section_data.columns]

            total_report.append(pd.merge(
                pd.merge(
                    region_data,
                    blck_stan_data,
                    on=['date', 'hour', 'region_id'],
                    how='left'
                ),
                section_data.reset_index(),
                on=['date', 'hour']
            ))
        return pd.concat(total_report).fillna(0.0)


class RegionFlowReport(AtsReport):
    report_type = 'overflow_region_spub'

    def _read_xls(self, wb: xlrd.book.Book):
        ws = wb.sheet_by_index(0)
        data = pd.DataFrame(
            data=ws._cell_values[6:],
            columns=[
                'region1_id',
                'region2_id',
                'hour',
                'flow'
            ]
        )
        data.replace('', null, inplace=True)
        data['region1_id'] = data.region1_id.replace(region_map).astype(int)
        data['region2_id'] = data.region2_id.replace(region_map).astype(int)
        data['hour'] = data.hour.astype(int)
        data['date'] = pd.to_datetime(self._context[0])
        self.data.append(data)


class SoReport(Report):
    url_tmpl = None

    def _read_csv(self, csv):
        raise NotImplementedError()

    def _download(self, url):
        self._read_csv(try_urlopen(url))

    def download(self, start_date, end_date=None):
        if end_date is None:
            days = [start_date]
        else:
            day_count = (end_date - start_date).days + 1
            days = [start_date + timedelta(n) for n in range(day_count)]       
            
        self.data = []
        for d in days:
            url = self.url_tmpl.format(report=self.report_type, date=d)
            self._download(url)

        return pd.concat(self.data)


class SOGenConsumReport(SoReport):
    report_type = 'GenConsum'
    url_tmpl = SO_REPORT_URL + 'startDate={date:%d.%m.%Y}&endDate={date:%d.%m.%Y}&territoriesIds=:530000,:550000,:600000,:610000,:630000,:840000&notCheckedColumnsNames='

    def _read_csv(self, csv):
        data = pd.read_csv(csv, sep=';', decimal=',')
        data.rename(inplace=True, columns={
            'INTERVAL': 'hour',
            'M_DATE': 'date',
            'POWER_SYS_ID': 'oes_id',
            'E_USE_FACT': 'vol_con_fact',
            'E_USE_PLAN': 'vol_con_plan',
            'GEN_FACT': 'vol_gen_fact',
            'GEN_PLAN': 'vol_gen_plan'
        })
        data.drop('PRICE_ZONE_ID', axis=1, inplace=True)
        data.replace(to_replace={'oes': {530000: 1, 550000: 2, 600000: 3, 610000: 4, 630000: 5, 840000: 7}}, inplace=True)
        self.data.append(data)


class SOForecastConsumSubReport(SoReport):
    report_type = 'ForecastConsumSubRf'
    url_tmpl = SO_REPORT_URL + 'date={date}'

    def _read_csv(self, csv):
        data = pd.read_csv(csv, sep=';', decimal=',')
        data.rename(inplace=True, columns={
            'sub_rf_id': 'region_id',
            'cons_value': 'vol_con_plan'
        })
        data.date = pd.to_datetime(data.date, format='%d.%m.%Y 0:00:00')
        self.data.append(data)


class SOBlockStationsReport(SoReport):
    report_type = 'PowerESPPByRegions'
    url_tmpl = SO_REPORT_URL + 'date={date}'
    
    def _read_csv(self, csv):
        data = pd.read_csv(csv, sep=';', decimal=',')
        data.rename(inplace=True, columns={
            'sub_rf_id': 'region_id',
            'Pbst': 'vol_gen_blockstan'
        })
        data.date = pd.to_datetime(data.date, format='%d.%m.%Y 0:00:00')
        self.data.append(data)
        

ats_reports = [
    DemandOfferCurveReport(),
    DguVolumeReport(),
    SectionReport(),
    BranchReport(),
    NodePriceReport(),
    RegionReport(),
    RegionFlowReport(),
]

so_reports = [
    SOGenConsumReport(),
    SOForecastConsumSubReport(),
    SOBlockStationsReport()
]


if __name__ == "__main__":
    r = RegionTotalReport()
    res = r.download(date(2018, 10, 1), date(2018, 10, 2))
    print(1)

    # # отчёты АТС
    # for r in ats_reports:
    #     t = time.time()
    #     r.download(days)
    #     print('{:0.1f} loaded'.format(time.time() - t), r.report_type)
    #
    # # отчёты СО
    # for r in so_reports:
    #     t = time.time()
    #     r.download(days)
    #     print('{:0.1f} loaded'.format(time.time() - t), r.report_type)
