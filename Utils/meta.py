class SingletonMeta(type):
    '''
    Metaclase para definir un Singleton. 
    Asegura que solo una instancia de la clase se cree.
    '''
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]