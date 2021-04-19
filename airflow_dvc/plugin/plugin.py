from airflow.plugins_manager import AirflowPlugin
from flask import request, Blueprint
from typing import List, Dict, Union, Set
from .git_url_parser import parse as parse_git_url

from airflow.models.dagbag import DagBag
from collections import defaultdict

from dataclasses import dataclass

from flask_appbuilder import BaseView as AppBuilderBaseView, expose
from airflow_dvc import DVCUpdateSensor, DVCUpdateOperator, DVCDownloadOperator, DVCHook


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


class AppBuilderDVCTargetsView(AppBuilderBaseView):
    @expose("/list", methods=["GET", "POST"])
    def list(self):
        operator_type = request.args.get('operator_type')
        if operator_type is None:
            operator_type = "all"
        elif operator_type not in ["downloads", "uploads", "sensors"]:
            operator_type = "all"

        operators: List[AnyDVCOperator] = []
        for dag in DagBag().dags.values():
            for task in dag.tasks:
                if isinstance(task, DVCDownloadOperator) or isinstance(task, DVCUpdateOperator) or isinstance(task,
                                                                                                              DVCUpdateSensor):
                    setattr(task, "dag", dag)
                    operators.append(task)

        repos: Dict[str, List[AnyDVCOperator]] = defaultdict(list)
        for operator in operators:
            repos[operator.dvc_repo].append(operator)
        targets_info: List[DVCTargetInfo] = []

        uploads_ref_files_count = 0
        downloads_ref_files_count = 0
        sensors_ref_files_count = 0

        dvc_diagram_free_id = 1
        dvc_diagram_nodes: Dict[str, dict] = dict()
        dvc_diagram_edges: Dict[int, Set[int]] = defaultdict(set)

        for target in repos.keys():
            repo_url_info = parse_git_url(target)
            target_name = f"{repo_url_info.owner}/{repo_url_info.repo}"
            if target_name in dvc_diagram_nodes:
                target_node_id = dvc_diagram_nodes[target_name].id
            else:
                target_node_id = dvc_diagram_free_id
                dvc_diagram_free_id += 1
            for operator in repos[target]:
                dag_id = operator.dag.dag_id
                if dag_id in dvc_diagram_nodes:
                    dag_node_id = dvc_diagram_nodes[dag_id].id
                else:
                    dag_node_id = dvc_diagram_free_id
                    dvc_diagram_free_id += 1
                add = False
                if isinstance(operator, DVCUpdateOperator) and operator_type in ["all", "uploads"]:
                    dvc_diagram_edges[dag_node_id].add(target_node_id)
                    add = True
                elif isinstance(operator, DVCDownloadOperator) and operator_type in ["all", "downloads"]:
                    dvc_diagram_edges[target_node_id].add(dag_node_id)
                    add = True
                if add:
                    dvc_diagram_nodes[dag_id] = dict(id=dag_node_id, label=dag_id, dag=True)
                    dvc_diagram_nodes[target_name] = dict(id=target_node_id, label=target_name, dag=False)

        for target in repos.keys():
            repo_url_info = parse_git_url(target)
            uploads = [op for op in repos[target] if isinstance(op, DVCUpdateOperator)]
            downloads = [op for op in repos[target] if isinstance(op, DVCDownloadOperator)]
            sensors = [op for op in repos[target] if isinstance(op, DVCUpdateSensor)]
            targets_info.append(DVCTargetInfo(
                dvc_repo=target,
                dvc_repo_owner=repo_url_info.owner,
                dvc_repo_name=repo_url_info.repo,
                upload_connected=any([isinstance(op, DVCUpdateOperator) for op in repos[target]]),
                download_connected=any([isinstance(op, DVCDownloadOperator) for op in repos[target]]),
                sensors_connected=any([isinstance(op, DVCUpdateSensor) for op in repos[target]]),
                uploads=uploads,
                downloads=downloads,
                sensors=sensors,
            ))

            uploads_ref_files_count += len([max(len(operator.affected_files), 1) for operator in uploads])
            downloads_ref_files_count += sum([max(len(operator.affected_files), 1) for operator in downloads])
            sensors_ref_files_count += len([max(len(operator.files), 1) for operator in sensors])

        return self.render_template(
            f"dvc/list.html",
            targets_info=targets_info,
            operator_type=operator_type,
            uploads_ref_files_count=uploads_ref_files_count,
            downloads_ref_files_count=downloads_ref_files_count,
            sensors_ref_files_count=sensors_ref_files_count,
            dvc_diagram_nodes=dvc_diagram_nodes,
            dvc_diagram_edges=dvc_diagram_edges,
        )


v_appbuilder_view = AppBuilderDVCTargetsView()
v_appbuilder_package = dict(
    name="DVC Operators",
    category="Browse",
    view=v_appbuilder_view,
)

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
    hooks = [DVCHook]

