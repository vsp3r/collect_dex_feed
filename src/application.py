import logging

class Application:
    """Standard application and process setup"""

    def __init__(self, name, class_name, *args):
        """Initialize object with its args, and setup logger"""
        self.logger = logging.getLogger("APP")
        self.name = name
        self.class_name = class_name
        self.args = args

        logging.basicConfig(filename=f'{name}.log', format="%(asctime)s [%(levelname)-7s] [%(name)s] %(message)s",
                            level=logging.INFO)

        self.logger.info(f'{name} started with config: {args}')


    def run(self):
        try:
            self.obj = self.class_name(self.args)
            self.obj.run()
        except KeyboardInterrupt:
            self.logger.debug(f'Keyboard INT detected at {self.name}')
        except Exception as e:
            self.logger.error(f'application exception {e} at {self.name}')
        finally:
            self.logger.info(f'closing app {self.name}')
            self.obj.close()
