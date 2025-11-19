# /// script
# dependencies = ["prefect"]
# ///

"""
A simple flow that says hello.
"""

from prefect import flow, get_run_logger, tags


# The name of the flow, `hello` is inferred from the function name by default
# The arguments to the flow are type annotated and Prefect will validate them at runtime
@flow(persist_result=True)
def hello(name: str = "Marvin"):
    get_run_logger().info(f"Hello, {name}! Is there anybody out there?")
    return {"moo": "foo"}


if __name__ == "__main__":
    # Run the flow
    hello()  # Output: "Hello, Marvin!"

    # Run the flow with a different argument
    hello("Arthur")  # Output: "Hello, Arthur!"

    # Run the flow with a "local" tag
    with tags("local"):
        hello()
