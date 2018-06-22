import logging, subprocess, os

def __get_datasource_parameter(name, value):
    return """<DataSourceParameter>
            <ParameterName>{name}</ParameterName>
            <ParameterValue>{value}</ParameterValue>
        </DataSourceParameter>
        """.format(
            name=name,
            value=value)

def __create_worksheet_task(worksheet_name, insertion_cell, data_source_type, data_source_parameters):

    return """<QAQCWorksheetTask>
        <WorksheetName>{worksheet_name}</WorksheetName>
        <InsertionCell>{insertion_cell}</InsertionCell>
        <DataSources>
        <DataSource xsi:type="{DataSourceType}">
            <QueryTemplateName>Compare Stocks</QueryTemplateName>
            <DatasourceParameters>
            {parameters}
            </DatasourceParameters>
        </DataSource>
        </DataSources>
    </QAQCWorksheetTask>
    """.format(
        worksheet_name=worksheet_name,
        insertion_cell=insertion_cell,
        datasource_type=datasource_type,
        datasource_parameters="".join(
            [__get_datasource_parameter(p["name"], p["value"])
            for p in datasource_parameters]))

def create_config(query_template_path, excel_template_path, excel_output_path, work_sheet_tasks):
         return """<?xml version="1.0" encoding="utf-8"?>
<QAQCTaskSet xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <QAQCSpreadsheetTasks>
        <QAQCSpreadsheetTask>
        <QueryTemplatePath>{query_template_path}</QueryTemplatePath>
        <ExcelTemplatePath>{excel_template_path}</ExcelTemplatePath>
        <ExcelOutputPath>{excel_output_path}</ExcelOutputPath>
        <WorksheetTasks>
        {worksheet_tasks}
        </WorksheetTasks>
    </QAQCSpreadsheetTask> 
   </QAQCSpreadsheetTasks>
</QAQCTaskSet>""".format(
    query_template_path=query_template_path,
    excel_template_path=excel_template_path,
    excel_output_path=excel_output_path,
    work_sheet_tasks="".join([
        __create_worksheet_task(
            worksheet_name=x["worksheet_name"],
            insertion_cell=x["insertion_cell"],
            data_source_type=x["data_source_type"],
            data_source_parameters=x["data_source_parameters"]) 
        for x in work_sheet_tasks]))

def run_qaqc(executable_path, query_template_path, excel_template_path, excel_output_path, work_sheet_tasks):

    xmlconfig_path = os.path.join(
        os.path.dirname(excel_output_path),
        os.path.splitext(os.path.basename(excel_output_path)) + "config.xml")

    config = create_config(
            query_template_path,
            excel_template_path,
            excel_output_path,
            work_sheet_tasks)

    with open(xmlconfig_path, 'w') as xmlconfig_file:
        xmlconfig_file.write(config)

    logging.info("""running qaqc: 
        executable_path: '{executable_path}'
        xmlconfig_path: '{xmlconfig_path}'
        query_template_path: '{query_template_path}'
        excel_template_path: '{excel_template_path}'
        excel_output_path: '{excel_output_path}'
        """.format(
            executable_path = executable_path,
            xmlconfig_path = xmlconfig_path,
            query_template_path=query_template_path,
            excel_template_path=excel_template_path,
            excel_output_path=excel_output_path))

    cmd = '{0} "{1}"'.format(executeable_path, xmlconfig_path)
    subprocess.check_call(cmd)
