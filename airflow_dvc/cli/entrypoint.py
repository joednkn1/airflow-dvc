import typer
import os, pathlib
from typing import Optional, Tuple
import configparser
import traceback

app = typer.Typer()

generator_app = typer.Typer()
app.add_typer(generator_app, name="generate")

AIRFLOW_HOME_VAR_NAME = "AIRFLOW_HOME"
AIRFLOW_CFG_VAR_NAME = "AIRFLOW_CONFIG"
AIRFLOW_CFG_FILE_NAME = "airflow.cfg"
AIRFLOW_DEFAULT_DAGS_DIR = "dags"


EXAMPLE_DAGS_DIRECTORY = os.path.abspath(
   os.path.join(
       os.path.dirname(__file__),
       "..",
       "..",
       "example",
       "dags",
   )
)


def get_airflow_dirs() -> Tuple[str, str]:
    airflow_home: Optional[str] = None
    if airflow_home is None and AIRFLOW_HOME_VAR_NAME in os.environ:
         airflow_home = os.path.abspath(os.environ[AIRFLOW_HOME_VAR_NAME])
    if airflow_home is None:
        airflow_cfg_path: Optional[str] = None
        if AIRFLOW_CFG_VAR_NAME in os.environ:
            airflow_cfg_path = os.environ[AIRFLOW_CFG_VAR_NAME]
        if airflow_cfg_path is None:
            search_cfg_path = os.path.abspath(".")
            while True:
                try:
                    cfg_path = os.path.join(search_cfg_path, AIRFLOW_CFG_FILE_NAME)
                    if os.path.exists(cfg_path):
                        airflow_cfg_path = cfg_path
                        break
                except Exception:
                    traceback.print_exc()
                new_search_cfg_path = os.path.dirname(search_cfg_path)
                if new_search_cfg_path == search_cfg_path:
                    break
                search_cfg_path = new_search_cfg_path
        if airflow_cfg_path is not None:
            try:
                config = configparser.ConfigParser()
                config.read(AIRFLOW_CFG_VAR_NAME)
                airflow_home = os.path.dirname(os.path.abspath(config['core']['dags_folder']))
            except Exception:
                traceback.print_exc()
    if airflow_home is not None:
        if os.path.exists(os.path.join(airflow_home, AIRFLOW_CFG_FILE_NAME)):
            config = configparser.ConfigParser()
            config.read(os.path.join(airflow_home, AIRFLOW_CFG_FILE_NAME))
            airflow_dag_dir = os.path.abspath(config['core']['dags_folder'])
        else:
            airflow_dag_dir = os.path.join(airflow_home, AIRFLOW_DEFAULT_DAGS_DIR)
    else:
        # Fallback
        # By default use current directory
        typer.echo(f'Failed to find Airflow home directory. Please specify {AIRFLOW_HOME_VAR_NAME} or {AIRFLOW_CFG_VAR_NAME} env variables. Or run the command anywhere near airflow.cfg file.')
        airflow_home = os.path.abspath(".")
        airflow_dag_dir = os.path.abspath(AIRFLOW_DEFAULT_DAGS_DIR)

    # Ensure all directories exist
    pathlib.Path(airflow_home).mkdir(parents=True, exist_ok=True)
    pathlib.Path(airflow_dag_dir).mkdir(parents=True, exist_ok=True)

    return airflow_home, airflow_dag_dir


@generator_app.command(f'example_dags')
def example_dags():
    _, dags_dir = get_airflow_dirs()
    typer.echo(f"Create example DAGs in {dags_dir} directory:")
    for filename in os.listdir(EXAMPLE_DAGS_DIRECTORY):
        if os.path.isfile(os.path.join(dags_dir, filename)):
            with open(os.path.join(dags_dir, filename), "w") as output:
                with open(os.path.join(EXAMPLE_DAGS_DIRECTORY, filename)) as input:
                    output.write(input.read())
                    typer.echo(f"Create example DAG {filename}")
    typer.echo("Done")


def run_cli():
    app()


if __name__ == '__main__':
    run_cli()
