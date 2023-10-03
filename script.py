import time
import typer
from asyncio import run
from rich.progress import Progress, SpinnerColumn, TextColumn
from kubernetes import client, config
from rich import print
from rich.table import Table
import requests
import asyncio
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


def send_request(thread_id, i):
    url = "http://127.0.0.1:30030/transaction"
    headers = {"Content-Type": "application/json"}
    data = {
        "key": f"key_{thread_id}_{i}",
        "value": i
    }
    try:
        requests.post(url, data=json.dumps(data), headers=headers)
    except Exception as e:
        print(f"Thread-{i} | Failed to send request: {str(e)}")

def thread_task(thread_id, n_requests):
    for i in range(n_requests):
        send_request(thread_id, i)

def start_server():
    url = 'http://localhost:30080/start'
    try:
        response = requests.get(url)
    except requests.RequestException as e:
        print(f"Failed to send request: {str(e)}")

def run_diagnostic(n_threads=1, n_requests=100):
    time.sleep(3)
    start_server()
    time.sleep(1)

    start = time.time()
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=thread_task, args=(i,n_requests))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    end = time.time()

    print(f"Finished {n_threads} threads with {n_requests} requests each in {end-start} seconds.")


@app.command()
def run():
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

                    nodes = v1.list_node()
                    table = Table(title="Nodes informations")

                    table.add_column("Name", justify="right", style="cyan", no_wrap=True)
                    table.add_column("CPU", style="magenta")
                    table.add_column("Memory", justify="right", style="green")

                    for node in nodes.items:
                        table.add_row(f"{node.metadata.name}", f"{node.status.capacity['cpu']}", f"{node.status.capacity['memory']}")
                   
                    print(table)

                    create_namespace(v1, namespace)

                    create_pod(v1, namespace, "themaimu/zebra-doctor-node:0.1", "none")

                    create_node_port_service(v1, namespace)

                    run_diagnostic()

                except KeyboardInterrupt:
                    print("Interrupted by user, shutting down")
                
                except NamspaceException as e:
                    print(e)

                finally: 
                    progress.add_task(description="Tear down...", total=None)
                    destroy_namespace(v1, namespace)

        

if __name__ == "__main__":
    app()