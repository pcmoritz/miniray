from functools import wraps
import inspect

import _ray

_global_context = _ray.context()

def is_function_or_method(obj):
    return inspect.isfunction(obj) or inspect.ismethod(obj)

class ActorMethod:
    def __init__(self, actor, name, method):
        self._name = name
        self._actor = actor
        @wraps(method)
        def remote_method_call(*args, **kwargs):
            return self._actor.submit(self._name, args, kwargs)
        self.remote = remote_method_call

class ActorHandle:
    def __init__(self, actor, modified_class):
        actor_methods = inspect.getmembers(modified_class, is_function_or_method)
        for name, method in actor_methods:
            setattr(self, name, ActorMethod(actor, name, method))


class ActorClass:

    def __init__(self, modified_class):
        self._modified_class = modified_class

    def remote(self, *args, **kwargs):
        actor = _global_context.make_actor(self._modified_class, args, kwargs)
        return ActorHandle(actor, self._modified_class)


def make_decorator():
    def decorator(function_or_class):
        if inspect.isclass(function_or_class):
            return ActorClass(function_or_class)
        else:
            raise ValueError(
                "The @ray.remote decorator must be applied to a class.")
    return decorator

def remote(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return make_decorator()(args[0])
    else:
        raise ValueError("Decorator with arguments currently not supported!")