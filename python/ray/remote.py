import inspect

class ActorHandle:
    def __init__(self, actor_id):
        self._ray_actor_id = actor_id


class ActorClass:

    def __init__(self, modified_class):
        self._modified_class = modified_class

    def remote(self, *args, **kwargs):

        return ActorHandle()



def make_actor(cls):
    class Class(cls):
        def __ray_terminate__(self):
            pass

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    return ActorClass(Class)

def make_decorator():
    def decorator(function_or_class):
        if inspect.isclass(function_or_class):
            return make_actor(function_or_class)
        else:
            raise ValueError(
                "The @ray.remote decorator must be applied to a class.")

def remote(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return make_decorator()(args[0])
    else:
        raise ValueError("Decorator with arguments currently not supported!")