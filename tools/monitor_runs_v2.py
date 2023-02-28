"""
Application for monitoring job runs and updating the job run metadata on S3.

This script is intended to be run in self contained application on Databricks.
It is not intended to be run as a notebook.

@TODO: [] - add more parameters to the JobRun class to capture more information about the job run.
       [] - look into extracting long running execution stages for a spark job.
       [] - latency metrics for the job run.
"""
from __future__ import annotations

import json
import sys
import typing as t
from dataclasses import dataclass

import boto3
import pyspark.sql.functions as sf
import requests
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from process.logger import logger
from process.models.schema import job_run_schema
from process.unpack import unpack_contents

# pylint: disable=unused-argument

# refer to typings/__builtins__.pyi for the type definitions and declarations
# for the builtin types spark, dbutils, etc.
# monkey patching dbutils to allow for local testing
spark: SparkSession = SparkSession.getActiveSession()

try:
    from process.common import get_dbutils

    dbutils: t.Callable = get_dbutils(spark)  # pylint: disable=undefined-variable

except ImportError as exc:
    raise ImportError('Unable to load environment variables through dbutils.') from exc


# class definitions for the job run metadata
class State:
    """
    Class to hold job run state information, including the state and message:

    >>> state = State("RUNNING", "Job is running", **{})
    >>> state.life_cycle_state
        'RUNNING'
    >>> state.state_message
        'Job is running'


    :param life_cycle_state : The state of the job run.
    :param state_message    : The message associated with the job run state.
    :param **kwargs         : Additional keyword arguments, for example,
                              `user_cancelled_or_timeout`.
    """

    def __init__(
        self,
        life_cycle_state: str,
        state_message: str,
        **kwargs: t.Any,
    ):
        self.life_cycle_state = life_cycle_state
        self.state_message = state_message
        self.__dict__.update(kwargs)


@dataclass
class JobRun:
    """
    Data class to hold job run information.
    """

    job_id: int
    run_id: int
    number_in_job: int
    state: State
    start_time: int
    setup_duration: str
    creator_user_name: str
    run_name: str
    run_page_url: str
    run_type: str

    # TODO: type validation for the job run class attributes
    def __post_init__(self) -> None:
        """
        Post initialization method to validate the job run.
        """
        pass

    @classmethod
    def from_dict(cls, data: dict[str, t.Any]) -> JobRun:
        """
        Create a job run from a dictionary.

        :param data : The dictionary containing the job run information.
        :return     : The job run.
        """
        return cls(
            job_id=data['job_id'],
            run_id=data['run_id'],
            number_in_job=data['number_in_job'],
            state=State(**data['state']),
            start_time=data['start_time'],
            setup_duration=data['setup_duration'],
            creator_user_name=data['creator_user_name'],
            run_name=data['run_name'],
            run_page_url=data['run_page_url'],
            run_type=data['run_type'],
        )

    def to_dict(self) -> dict[str, t.Any]:
        """
        Convert the job run to a dictionary.

        :return : The dictionary containing the job run information.
        """
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'number_in_job': self.number_in_job,
            'state': {
                'life_cycle_state': self.state.life_cycle_state,
                'state_message': self.state.state_message,
            },
            'start_time': self.start_time,
            'setup_duration': self.setup_duration,
            'creator_user_name': self.creator_user_name,
            'run_name': self.run_name,
            'run_page_url': self.run_page_url,
            'run_type': self.run_type,
        }

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=4)


class HeaderTagElement:
    """
    Class to hold header tag elements.

    :param id       : The id of the header tag element.
    :param width    : The width of the header tag element.
    :param column   : The column of the header tag element.
    """

    def __init__(self, id: str, width: int, column: str):
        self.id = id
        self.width = width
        self.column = column


def main(
        metadata_table: str,
        bucket_name: str,
        html_file_path: str,
):
    """
    Extracts the job run metadata from the Databricks REST API and writes it to
    a Delta table.

    :param metadata_table         : The name of the Delta table to write to.
    :param job_runs_table_path    : The path to the Delta table to write the job
                                    run metadata to.
    :param bucket_name            : The name of the S3 bucket to write the HTML
                                    file to.
    :param html_file_path         : The path to the HTML file to write the job
                                    run metadata to.
    """
    # # inherit the logger from the spark session
    # logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    # set spark configuration
    spark_defaults = {
        'spark.databricks.delta.formatCheck.enabled' : 'false',
        'spark.databricks.io.cache.enabled' : 'true',
        'spark.databricks.delta.retentionDurationCheck.enabled' : 'false',
        'spark.sql.caseSensitive' : 'true',
    }

    logger.info('Setting spark configuration')
    for key, value in spark_defaults.items():
        spark.conf.set(key, value)

    html_plain_tag = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Jobs Page</title>
            <meta charset="utf-8" />
            <link
            rel="stylesheet"
            href="https://use.fontawesome.com/releases/v5.8.2/css/all.css"
            />
            <link
            rel="stylesheet"
            href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap"
            />
            <link
            href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.4.1/css/bootstrap.min.css"
            rel="stylesheet"
            />
            <link
            href="https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.18.0/css/mdb.min.css"
            rel="stylesheet"
            />
            <link
            rel="stylesheet"
            type="text/css"
            href="https://cdn.datatables.net/1.10.21/css/jquery.dataTables.css"
            />
            <script
            type="text/javascript"
            src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.4.1/jquery.min.js"
            ></script>
            <script
            type="text/javascript"
            src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.4/umd/popper.min.js"
            ></script>
            <script
            type="text/javascript"
            src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/4.4.1/js/bootstrap.min.js"
            ></script>
            <script
            type="text/javascript"
            src="https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.18.0/js/mdb.min.js"
            ></script>
            <script
            type="text/javascript"
            charset="utf8"
            src="https://cdn.datatables.net/1.10.21/js/jquery.dataTables.js"
            ></script>
            <style>
            table.dataTable thead .sorting:after,
            table.dataTable thead .sorting:before,
            table.dataTable thead .sorting_asc:after,
            table.dataTable thead .sorting_asc:before,
            table.dataTable thead .sorting_asc_disabled:after,
            table.dataTable thead .sorting_asc_disabled:before,
            table.dataTable thead .sorting_desc:after,
            table.dataTable thead .sorting_desc:before,
            table.dataTable thead .sorting_desc_disabled:after,
            table.dataTable thead .sorting_desc_disabled:before {
                bottom: 0.5em;
            }
            table a {
                margin: 0;
                color: #007bff !important;
            }
            </style>
            <script>
            $(document).ready(function () {
                $("#dtBasicExample").DataTable();
                $(".dataTables_length").addClass("bs-select");
            });
            </script>
        </head>
        <body>
            <div class="container" style="margin-left: initial !important">
            <h2>%s</h2>
            <div class="row">
                <div class="col-sm-12">%s</div>
            </div>
            </div>
        </body>
        </html>
    """

    logger.info('computing and aggregating job runs metadata')
    job_runs_df = aggregate_job_runs_df(spark)

    logger.info('building html table')
    html_formatted_job_runs = html_plain_tag % (
        'Job Runs',
        build_table(get_tabe_header_tag(), job_runs_table_body_tags(job_runs_df)),
    )

    # # check if the delta table exists and create it if it doesn't
    # if not DeltaTable.isDeltaTable(spark, job_runs_table_path):
    #     spark.sql(
    #         f"""
    #         CREATE TABLE {metadata_table}
    #         USING DELTA
    #         LOCATION '{job_runs_table_path}'
    #         """
    #     )

    # write the job run metadata to the delta table and partition it by date
    logger.info('writing job runs metadata to delta table')
    (
        job_runs_df
        .write
        .partitionBy('date')
        .format('delta')
        .mode('append')
        .save(metadata_table)
    )

    # s3 client
    s3_client = boto3.client('s3')

    # upload the html file to s3
    logger.info('uploading html file to s3')
    s3_client.put_object(
        Body=html_formatted_job_runs,
        Bucket=bucket_name,
        Key=html_file_path,
        ContentType='text/html',
    )


# get the list of runs using the Databricks API
def get_runs_list() -> str:
    """
    Get the list of runs using the Databricks API.

    :return : The list of runs.
    """
    api_url = 'https://tfs-edp.cloud.databricks.com'
    access_token = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    )
    # search API using generated the generated access token
    api_endpoint = f'{api_url}/api/2.0/jobs/runs/list'
    params = {
        'active_only': 'true',
        'limit': '150',
    }
    reponse = unpack_contents(
        response=requests.get(
            api_endpoint,
            headers={'Authorization': 'Bearer ' + access_token},
            params=params,
        ),
    )
    response_str = reponse.text
    return response_str


# udf to get the table row tag
@sf.udf
def get_table_row_tag(
        job_id: int,
        run_id: int,
        run_name: str,
        run_page: str,
        start_time: str,
        created_by: str,
) -> str:
    """
    Get the table row tag and apply the spark UDF to the row tag.

    :param job_id      : The job id.
    :param run_id      : The run id.
    :param run_name    : The run name.
    :param run_page    : The run page.
    :param start_time  : The start time.
    :param created_by  : The created by.
    :return            : The table row tag.
    """
    return build_table_row_tag(
        job_id, run_id, run_name, run_page, start_time, created_by,
    )


# parse the JSON payload
def parse_json(payload: str) -> list[JobRun]:
    """
    Parse the JSON payload and return a list of JobRun objects.

    :param payload : The JSON payload.
    :return        : The list of JobRun objects.
    """
    # TODO: add error handling
    runs = []
    for run in json.loads(payload)['runs']:  # recursion to get the nested values
        runs.append(
            JobRun(
                run['job_id'], run['run_id'], run['number_in_job'], run['state'],
                run['start_time'], run['setup_duration'], run['creator_user_name'],
                run['run_name'], run['run_page_url'],run['run_type'],
            ),
        )

    if len(runs) == 0:
        raise ValueError('No job runs found.')

    return runs


# job runs dataframe
def aggregate_job_runs_df(spark: SparkSession) -> DataFrame:
    """
    aggregate the job runs dataframe and partition it by date.

    :return : The job runs dataframe.
    """
    date_format = 'dd-MM-yyyy HH:mm:ss'
    runs = parse_json(get_runs_list())

    job_runs_df = spark.createDataFrame(data=runs, schema=job_run_schema)
    job_runs_df = (
        job_runs_df
        .withColumn(
            'run_start_time',
            sf.from_unixtime(job_runs_df.start_time / 1000, date_format),
        )
        .withColumn(
            'date',
            # convert bigint to timestamp
            sf.to_date(
                sf.from_unixtime(
                    sf.col('start_time') / 1000,
                    'yyyy-MM-dd',
                ),
            ),
        )
    )
    job_runs_df.show(truncate=False)

    refined_job_runs_df = (
        job_runs_df
        .withColumn(
            'table_row_tag',
            get_table_row_tag(
                sf.col('job_id'),
                sf.col('run_id'),
                sf.col('run_name'),
                sf.col('run_page_url'),
                sf.col('run_start_time'),
                sf.col('creator_user_name'),
            ),
        )
    )

    return refined_job_runs_df


# get the table body tags for the job runs
def job_runs_table_body_tags(job_runs_df: DataFrame) -> list[str]:
    """
    Get the table body tags for the job runs.

    :param job_runs_df : The job runs dataframe.
    :return            : The list of table body tags.
    """
    runs_table_body_tags = (
        job_runs_df
        .filter(job_runs_df.run_type == 'JOB_RUN')  # TODO: remove filter?
        .select('table_row_tag')
        .rdd.map(lambda x: x[0])
        .collect()
    )

    return runs_table_body_tags


# table headers
def build_table_header(headers: list[HeaderTagElement]) -> str:
    """
    Build a table header tag.

    :param headers : The list of header tag elements.
    :return        : The table header tag.
    """
    tr_tag = '<tr role="row">{}</tr>'
    th_tag = """
        <th
        class="th-sm sorting_asc"
        tabindex="0"
        aria-controls="dtBasicExample"
        rowspan="1"
        colspan="1"
        aria-sort="ascending"
        aria-label="{} : activate to sort column descending"
        style="width: {}px;"
        >
        {}
        </th>
    """
    header_tags_arr = [
        th_tag.format(hte.id, hte.width, hte.column) for i, hte in enumerate(headers)
    ]
    header_tags = ''.join(header_tags_arr)
    return tr_tag.format(header_tags)


# table data
def build_table_data(value: str) -> str:
    """
    Build a table data tag.

    :param value : The value to be displayed in the table data tag.
    :return      : The table data tag.
    """
    return f'<td>{value}</td>'


# table row
def build_table_row(row_values_arr: list[str]) -> str:
    """
    Build a table row tag.

    :param row_values_arr : The list of values to be displayed in the table row tag.
    :return               : The table row tag.
    """
    td_tags = [build_table_data(value) for value in row_values_arr]
    row_values = ''.join(td_tags)
    return f'<tr role="row" class="even">{row_values}</tr>'


# table body
def build_table(table_header_tag: str, table_body_tags: list[str]) -> str:
    """
    Build a table tag.

    :param table_header_tag : The table header tag.
    :param table_body_tags  : The list of table body tags.
    :return                 : The table tag.
    """
    table_tag = """
        <table
        id="dtBasicExample"
        class="table table-striped table-bordered table-sm dataTable"
        cellspacing="0"
        role="grid"
        aria-describedby="dtBasicExample_info"
        width="100%"
        style="width: 100%"
        >
        <thead>
            {}
        </thead>
        <tbody>
            {}
        </tbody>
        </table>
    """
    table_body_elements = ''.join(table_body_tags)
    return table_tag.format(table_header_tag, table_body_elements)


# anchor tag
def build_anchor(name: str, href: str) -> str:
    """
    Build an anchor tag.

    :param name : The name of the anchor tag.
    :param href : The href of the anchor tag.
    :return     : The anchor tag.
    """
    return f'<a href="{href}" target="_blank" style>{name}</a>'


def build_table_row_tag(
        job_id: int,
        run_id: int,
        run_name: str,
        run_page: str,
        start_time: str,
        created_by: str,
) -> str:
    """
    Build a table row tag for HTML display.

    :param job_id      : The job id.
    :param run_id      : The run id.
    :param run_name    : The run name.
    :param run_page    : The run page.
    :param start_time  : The start time.
    :param created_by  : The created by.
    :return            : The table row tag.
    """
    job_run_as_list = [
        str(job_id),
        build_anchor(str(run_id), run_page),
        run_name,
        start_time,
        created_by,
    ]
    table_row_tag = build_table_row(job_run_as_list)
    return table_row_tag


def get_header_columns() -> list[HeaderTagElement]:
    """
    Get the header columns.

    :return : The list of header tag elements.
    """
    headers = [
        HeaderTagElement('job_id', 85, 'Job ID'),
        HeaderTagElement('run_page', 85, 'Run Page'),
        HeaderTagElement('run_name', 255, 'Run Name'),
        HeaderTagElement('start_time', 140, 'Start Time'),
        HeaderTagElement('creator_user_name', 200, 'Created By'),
    ]

    return headers


# table header and body tags
def get_tabe_header_tag() -> str:
    """
    Get the table header tag.

    :return : The table header tag.
    """
    table_header_tag = build_table_header(get_header_columns())
    return table_header_tag


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main(sys.argv[1], sys.argv[2], sys.argv[3])
