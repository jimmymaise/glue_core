import os

from opus_glue_core.local_deploy_libs.deploy_handler import DeployHandler

etl_local_path = os.path.dirname(os.path.realpath(__file__))


class Deploy:
    def __init__(self):
        self.deploy_handler = DeployHandler(etl_local_path=etl_local_path)

    def main(self):
        self.deploy_handler.deploy_glue()


if __name__ == '__main__':
    Deploy().main()
