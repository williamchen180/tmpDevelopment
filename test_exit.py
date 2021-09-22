import atexit

class Test:
    def __init__(self):



        print('__init__ of this class')

        atexit.register(self.cleanup)


    def __exit__(self, exc_type, exc_val, exc_tb):

        print('__exit__ of this class')

    def __enter__(self):

        print('this is enter')

    def cleanup(self):
        print(f'cleanup {self.__class__.__name__}')




t = Test()


# quit()