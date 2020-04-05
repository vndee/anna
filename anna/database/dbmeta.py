class DatabaseMeta(type):
    """
    Database meta class
    """
    def __new__(cls, name, bases, body):
        if 'insert' not in body:
            raise TypeError(f'{name} must be implemented insert method.')
        if 'update' not in body:
            raise TypeError(f'{name} must be implemented update method.')

        return super().__new__(cls, name, bases, body)
