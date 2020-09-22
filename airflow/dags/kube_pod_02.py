from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

volume_mount_nb = VolumeMount(name='nb-data',
                            mount_path='/opt/airflow/nb',
                            sub_path=None,
                            read_only=False)

volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-nfs-pvc-nb-01'
        }
}
volume_nb = Volume(name='nb-data', configs=volume_config)

dag = DAG(
    'kubernetes_pod_operator_02', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)

passing = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python"],
                          arguments=["/opt/airflow/nb/airflow_nb.py"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          image_pull_policy="IfNotPresent",
                          reattach_on_restart=True,
                          is_delete_operator_pod=True,
                          volumes=[volume_nb],
                          volume_mounts=[volume_mount_nb],
                          dag=dag
                          )


passing.set_upstream(start)
