# /// script
# dependencies = ["prefect"]
# ///

"""
A simple flow that says hello.
"""

from datetime import date
from prefect import flow, get_run_logger, tags
from prefect.artifacts import create_table_artifact


# The name of the flow, `hello` is inferred from the function name by default
# The arguments to the flow are type annotated and Prefect will validate them at runtime
@flow(persist_result=True)
def hello(name: str = "Marvin"):
    get_run_logger().info(f"Hello, {name}! Is there anybody out there?")
    today = date.today()

    create_table_artifact(
        key="files",
        table={
            'foo': True,
            'when': today
        },
        description="Flow execution results"
    ) 

    return {"moo": "foo"}


if __name__ == "__main__":
    # Run the flow
    hello()  # Output: "Hello, Marvin!"

    # Run the flow with a different argument
    hello("Arthur")  # Output: "Hello, Arthur!"

    # Run the flow with a "local" tag
    with tags("local"):
        hello()
