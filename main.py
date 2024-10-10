import os
from datetime import datetime

from method_manager import MethodManager

batch_size = 100000

file_name = "eventstore.json"
# file_name = "securebank_eventstore.json"

file = os.path.join("data", file_name)
step_clear_db = True
step_populate_graph = True


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    print("Started at =", datetime.now().strftime("%H:%M:%S"))
    methods = MethodManager(file=file)

    if step_clear_db:
        methods.clear_database()

    if step_populate_graph:
        methods.load_and_import_json()

    methods.finish_and_save()
    methods.print_statistics()


if __name__ == "__main__":
    main()
