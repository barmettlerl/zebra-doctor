# from kubernetes import client, config

# # Configs can be set in Configuration class directly or using helper utility
# config.load_kube_config()

# v1 = client.CoreV1Api()
# v1.create_namespace(body=client.V1Namespace(metadata=client.V1ObjectMeta(name="zebra-zoo")))
# print("Namespace created.")
# namespaces = v1.list_namespace()
# for ns in namespaces.items:
#     print(ns.metadata.name)


# print (namespaces)
# ret = v1.list_pod_for_all_namespaces(watch=False)
# for i in ret.items:
#     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
import time
import typer
from asyncio import run as create_task, get_event_loop
from rich.progress import Progress, SpinnerColumn, TextColumn
import asyncio
from kubernetes import client, config
from rich import print

class NamspaceException(Exception):
    pass

app = typer.Typer()

def create_namespace(v1):
    try:
        v1.create_namespace(body=client.V1Namespace(metadata=client.V1ObjectMeta(name="zebra-zoo")))
        print("- Namespace [bold]zebra-zoo[/bold] created.")

    except Exception:
        raise NamspaceException("[bold red] Namespace [bold]zebra-zoo[/bold] already exists [/bold red].")
    
def destroy_namespace(v1):
    try:
        v1.delete_namespace(name="zebra-zoo", body=client.V1DeleteOptions())
        print("- Namespace [bold]zebra-zoo[/bold] deleted.")

    except Exception:
        raise NamspaceException("[bold red]Namespace [bold]zebra-zoo[/bold] does not exists[/bold red].")

@app.command()
def run():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
                try:
                    progress.add_task(description="Build Up...", total=None)

                    config.load_kube_config()
                    v1 = client.CoreV1Api()

                    create_namespace(v1)

                except KeyboardInterrupt:
                    print("Interrupted by user, shutting down")
                
                except NamspaceException as e:
                    print(e)

                finally: 
                     destroy_namespace(v1)

        

if __name__ == "__main__":
    app()