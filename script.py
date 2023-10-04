import time
from typing import Annotated
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from kubernetes import client, config
from rich import print
from rich.table import Table
import requests
import threading
import json

class NamspaceException(Exception):
    pass

app = typer.Typer()

def create_namespace(v1, namespace):
    try:
        v1.create_namespace(body=client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        print("- Namespace [bold]zebra-zoo[/bold] created.")

    except Exception:
        raise NamspaceException("[bold red] Namespace [bold]zebra-zoo[/bold] already exists [/bold red].")
    
def destroy_namespace(v1, namespace):
    try:
        v1.delete_namespace(name=namespace, body=client.V1DeleteOptions())
        namespaces = v1.list_namespace()
        # api_response = list(filter(lambda ns: ns.metadata.name == namespace, namespaces.items)).pop()
        # print(api_response.status)
        while True:
            namespaces = v1.list_namespace()
            api_response = list(filter(lambda ns: ns.metadata.name == namespace, namespaces.items))
            if len(api_response) == 0:
                break
            api_response = api_response.pop()
            if api_response == None or api_response.status.phase != 'Terminating':
                break
            time.sleep(0.01)
        print("- Namespace [bold]zebra-zoo[/bold] deleted.")

    except Exception:
        raise NamspaceException("[bold red]Namespace [bold]zebra-zoo[/bold] does not exists[/bold red].")
    
def create_pod(v1, namespace, image, test_mode):
    pod_manifest = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'test-pod',
                'labels': {
                    'app': 'test-app',
                }
            },
            'spec': {
                'containers': [{
                    'image': image,
                    'name': 'test-container',
                    'pod-running-timeout': '5m0s',
                    'env': [
                        {'name': 'TEST_MODE', 'value': test_mode},
                    ],
                    'ports': [{
                        'containerPort': 3000,
                    }, {
                        'containerPort': 8000,
                    }]
                }],
                'restartPolicy': 'Never',
            }
        }
    try:
        v1.create_namespaced_pod(body=pod_manifest, namespace=namespace)

        while True:
            api_response = v1.read_namespaced_pod(name="test-pod", namespace=namespace)
            if api_response.status.phase != 'Pending':
                break
            time.sleep(0.01)
        
        print("- Pods created.")
    except:
        raise Exception("Pod could not be started.")
    
def create_node_port_service(v1, namespace):
    service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name="test-service"),
        spec=client.V1ServiceSpec(
            selector={"app": "test-app"},
            ports=[
                client.V1ServicePort(name="server", port=8000, target_port=8000, node_port=30080),
                client.V1ServicePort(name="database-runner", port=3000, target_port=3000, node_port=30030),
            ],
            type="NodePort"
        )
    )
    
    try:
        v1.create_namespaced_service(namespace=namespace, body=service)
        print("- Service created.")
        print("- Service is available at http://localhost:30080 for controlling and at http://127.0.0.1:30030/ for testing.")
    except:
        raise Exception("Service could not be started.")

def send_request(thread_id, i, n_transactions_per_request):
    url = "http://127.0.0.1:30030/transaction"
    headers = {"Content-Type": "application/json"}
    data = []
    for j in range(n_transactions_per_request):
        data.append({
            "key": f"key_{thread_id}_{i}_{j}",
            "value": j
        })
    try:
        res = requests.post(url, data=json.dumps(data), headers=headers)
    except Exception as e:
        print(f"Thread-{i} | Failed to send request: {str(e)}")

def thread_task(thread_id, n_requests, n_transactions_per_request):
    for i in range(n_requests):
        send_request(thread_id, i, n_transactions_per_request)

def start_server(test_mode):
    url = 'http://localhost:30080/start'
    try:
        requests.post(url, data=json.dumps({"test_mode": test_mode}), headers={"Content-Type": "application/json"})
    except requests.RequestException as e:
        print(f"Failed to send request: {str(e)}")

def stop_server():
    url = 'http://localhost:30080/stop'
    try:
        requests.get(url, headers={"Content-Type": "application/json"})
    except requests.RequestException as e:
        print(f"Failed to send request: {str(e)}")

def run_diagnostic(n_threads, n_requests, n_transactions_per_request):
    time.sleep(3)
    start_server("NoBackup")
    time.sleep(2)

    start = time.time()
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=thread_task, args=(i,n_requests, n_transactions_per_request))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    end = time.time()

    no_backup_time = end-start

    stop_server()

    time.sleep(1)

    start_server("SerializeBackup")

    start = time.time()
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=thread_task, args=(i,n_requests, n_transactions_per_request))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    end = time.time()

    serialize_backup_time = end-start

    table = Table(title="Benchmarks")

    table.add_column("Test Mode", justify="right", style="cyan", no_wrap=True)
    table.add_column("# threads", style="magenta")
    table.add_column("# requests", style="magenta")
    table.add_column("# transactions per request", style="magenta")
    table.add_column("Time", justify="right", style="green")

    table.add_row("NoBackup", f"{n_threads}", f"{n_requests}", f"{n_transactions_per_request}", "{:.3f}".format(no_backup_time))
    table.add_row("SerializeBackup", f"{n_threads}", f"{n_requests}", f"{n_transactions_per_request}", "{:.3f}".format(serialize_backup_time))       

    print(table)

def show_cluster_info(v1):
    nodes = v1.list_node()
    table = Table(title="Nodes informations")

    table.add_column("Name", justify="right", style="cyan", no_wrap=True)
    table.add_column("CPU", style="magenta")
    table.add_column("Memory", justify="right", style="green")

    for node in nodes.items:
        table.add_row(f"{node.metadata.name}", f"{node.status.capacity['cpu']}", f"{node.status.capacity['memory']}")
                   
    print(table)

@app.command()
def run(
        n_threads: Annotated[int, typer.Option(help="Number of thread of parallel request done")]=6, 
        n_requests: Annotated[int, typer.Option(help="Number of requests per thread")]=1000, 
        n_transactions_per_request: Annotated[int, typer.Option(help="Number of transactions per request")]=1000,
    ):
        """
        Run the test programm.
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
                namespace = "zebra-zoo"
                try:
                    progress.add_task(description="Build Up...", total=None)

                    config.load_kube_config()
                    v1 = client.CoreV1Api()

                    show_cluster_info(v1)

                    create_namespace(v1, namespace)

                    create_pod(v1, namespace, "themaimu/zebra-doctor-node:0.3", "none")

                    create_node_port_service(v1, namespace)

                    run_diagnostic(n_threads, n_requests, n_transactions_per_request)

                except KeyboardInterrupt:
                    print("Interrupted by user, shutting down")
                
                except NamspaceException as e:
                    print(e)

                finally: 
                    progress.add_task(description="Tear down...", total=None)
                    destroy_namespace(v1, namespace)

        

if __name__ == "__main__":
    app()