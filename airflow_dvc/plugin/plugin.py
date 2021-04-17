from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow import configuration as conf
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagBag
from flask import render_template_string, request, Markup, Blueprint
from airflow.utils import timezone
from typing import List, Dict, Union
from .git_url_parser import parse as parse_git_url

from dataclasses import dataclass

from airflow_dvc import DVCUpdateSensor, DVCUpdateOperator, DVCDownloadOperator

trigger_template = """  
{% extends "airflow/master.html" %}
{% block body %}
    <a href="/home">Home</a>
      {% if messages %}
        <ul class=flashes>
        {% for message in messages %}
          <li>{{ message }}</li>
        {% endfor %}
        </ul>
      {% endif %}
    <h1>Manual Trigger</h1>
    <div class="widget-content">
       <form id="triggerForm" method="post">
          <label for="dag">Select a dag:</label>
          <select name="dag" id="selected_dag">
              <option value=""></option>
              {%- for dag_id, dag_arguments in dag_data.items() %}
              <option value="{{ dag_id }}" {% if dag_id in selected %}selected="selected"{% endif %}>{{ dag_id }}</option>
              {%- endfor %}
          </select>
          <div id="dag_options">
              {%- for dag_id, dag_arguments in dag_data.items() %}
                  <div id="{{ dag_id }}" style='display:none'>
                    {% if dag_arguments %}
                        <b>Arguments to trigger dag {{dag_id}}:</b><br>
                    {% endif %}
                    {% for dag_argument_name, _ in dag_arguments.items() %}
                        <input type="text" id="{{ dag_argument_name }}" name="{{dag_id}}-{{ dag_argument_name }}" placeholder="{{ dag_argument_name }}" ><br>
                    {% endfor %}
                  </div>
              {%- endfor %}
          </div>
          <br>
          <input type="submit" value="Trigger" class="btn btn-secondary">
        {% if csrf_token %}
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        {% endif %}
       </form>
    </div>
{% endblock %}
"""


def trigger(dag_id, trigger_dag_conf):
    """Function that triggers the dag with the custom conf"""
    execution_date = timezone.utcnow()

    dagrun_job = {
        "dag_id": dag_id,
        "run_id": f"manual__{execution_date.isoformat()}",
        "execution_date": execution_date,
        "replace_microseconds": False,
        "conf": trigger_dag_conf
    }
    r = trigger_dag(**dagrun_job)
    return r


# # if we dont have RBAC enabled, we setup a flask admin View
# from flask_admin import BaseView, expose
# class FlaskAdminDVCTargetsView(BaseView):
#     @expose("/", methods=["GET", "POST"])
#     def list(self):
#         if request.method == "POST":
#             print(request.form)
#             trigger_dag_id = request.form["dag"]
#             trigger_dag_conf = {k.replace(trigger_dag_id, "").lstrip("-"): v for k, v in request.form.items() if k.startswith(trigger_dag_id)}
#             dag_run = trigger(trigger_dag_id, trigger_dag_conf)
#             messages = [f"Dag {trigger_dag_id} triggered with configuration: {trigger_dag_conf}"]
#             dag_run_url = DAG_RUN_URL_TMPL.format(dag_id=dag_run.dag_id, run_id=dag_run.run_id)
#             messages.append(Markup(f'<a href="{dag_run_url}" target="_blank">Dag Run url</a>'))
#             dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
#             return render_template_string(trigger_template, dag_data=dag_data, messages=messages)
#         else:
#             dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
#             return render_template_string(trigger_template, dag_data=dag_data)
# v = FlaskAdminDVCTargetsView(category="Extra", name="Manual Trigger")


from airflow.models.dagbag import DagBag
from collections import defaultdict

AnyDVCOperator = Union[DVCDownloadOperator, DVCUpdateOperator, DVCUpdateSensor]

@dataclass(frozen=True)
class DVCTargetInfo:
    dvc_repo: str
    dvc_repo_owner: str
    dvc_repo_name: str
    upload_connected: bool
    download_connected: bool
    sensors_connected: bool
    uploads: List[DVCUpdateOperator]
    downloads: List[DVCDownloadOperator]
    sensors: List[DVCUpdateSensor]

# If we have RBAC, airflow uses flask-appbuilder, if not it uses flask-admin
from flask_appbuilder import BaseView as AppBuilderBaseView, expose
class AppBuilderDVCTargetsView(AppBuilderBaseView):
    @expose("/", methods=["GET", "POST"])
    def list(self):

        operators: List[AnyDVCOperator] = []

        for dag in DagBag().dags.values():
            for task in dag.tasks:
                if isinstance(task, DVCDownloadOperator) or isinstance(task, DVCUpdateOperator) or isinstance(task, DVCUpdateSensor):
                    operators.append(task)

        repos: Dict[str, AnyDVCOperator] = defaultdict(list)
        for operator in operators:
            repos[operator.dvc_repo].append(operator)

        targets_info: List[DVCTargetInfo] = []
        for target in repos.keys():
            repo_url_info = parse_git_url(target)
            targets_info.append(DVCTargetInfo(
                dvc_repo=target,
                dvc_repo_owner=repo_url_info.owner,
                dvc_repo_name=repo_url_info.repo,
                upload_connected=any([isinstance(op, DVCUpdateOperator) for op in repos[target]]),
                download_connected=any([isinstance(op, DVCDownloadOperator) for op in repos[target]]),
                sensors_connected=any([isinstance(op, DVCUpdateSensor) for op in repos[target]]),
                uploads=[op for op in repos[target] if isinstance(op, DVCUpdateOperator)],
                downloads=[op for op in repos[target] if isinstance(op, DVCDownloadOperator)],
                sensors=[op for op in repos[target] if isinstance(op, DVCUpdateSensor)],
            ))

        return self.render_template("dvc/index.html", targets_info=targets_info)
        # if request.method == "POST":
        #     print(request.form)
        #     trigger_dag_id = request.form["dag"]
        #     trigger_dag_conf = {k.replace(trigger_dag_id, "").lstrip("-"): v for k, v in request.form.items() if k.startswith(trigger_dag_id)}
        #     dag_run = trigger(trigger_dag_id, trigger_dag_conf)
        #     messages = [f"Dag {trigger_dag_id} triggered with configuration: {trigger_dag_conf}"]
        #     dag_run_url = DAG_RUN_URL_TMPL.format(dag_id=dag_run.dag_id, run_id=dag_run.run_id)
        #     messages.append(Markup(f'<a href="{dag_run_url}" target="_blank">Dag Run url</a>'))
        #     dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
        #     return render_template_string(trigger_template, dag_data=dag_data, messages=messages)
        # else:
        #     dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
        #     return render_template_string(trigger_template, dag_data=dag_data)


v_appbuilder_view = AppBuilderDVCTargetsView()
v_appbuilder_package = {"name": "DVC Targets",
                        "category": "DVC",
                        "view": v_appbuilder_view}

dag_creation_manager_bp = Blueprint(
    "dag_creation_manager_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/dvc"
)

# Defining the plugin class
class DVCPlugin(AirflowPlugin):
    name = "dvc_plugin"
    flask_blueprints = [dag_creation_manager_bp]
    admin_views = [] # if we dont have RBAC we use this view and can comment the next line
    appbuilder_views = [v_appbuilder_package] # if we use RBAC we use this view and can comment the previous line


