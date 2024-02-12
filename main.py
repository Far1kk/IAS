import parser_wb
import processing
from exporter import DataExporter

if __name__ == '__main__':
    app_wb = parser_wb.WildBerriesParser()
    app_wb.run_parser()
    path = app_wb.result_path
    date = app_wb.run_date
    region = app_wb.user_region.lower()
    query = app_wb.user_query
    print('Параметры передачи', path, date, region, query)
    app_proc = processing.processing(path, date, region, query)
    P = app_proc.run_analytic()
    exporter = DataExporter()
    # Экспортируем датафреймы в базу данных PostgreSQL
    exporter.export_to_database([app_proc.date_db, app_proc.product_db, app_proc.region_db, app_proc.query_db,
                                 app_proc.stock_db, app_proc.fact_product_db, app_proc.fact_stock_db,
                                 app_proc.fact_region_db],
                                ['date', 'product', 'region', 'query', 'stock',
                                 'fact_product', 'fact_stock', 'fact_region'])

    # Экспортируем датафреймы в Excel-файлы
    # exporter.export_to_excel([df1, df2], ['file1.xlsx', 'file2.xlsx'])