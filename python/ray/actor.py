from functools import wraps
import inspect
import weakref

import ray._ray as _ray

_global_context = _ray.context()

def is_function_or_method(obj):
    return inspect.isfunction(obj) or inspect.ismethod(obj)

class ActorMethod:
    def __init__(self, name, actor, method):
        self._name = name
        self._actor = weakref.ref(actor)
        def remote_method_call(*args, **kwargs):
            return self._actor().submit(self._name, args, kwargs)
        for attr, value in method.items():
            setattr(remote_method_call, attr, value)
        self.remote = remote_method_call

class ActorHandle:
    def __init__(self, actor, actor_methods):
        self._actor = actor
        self._actor_methods = actor_methods

        for name, method in self._actor_methods.items():
            setattr(self, name, ActorMethod(name, self._actor, method))

    def __reduce_ex__(self, protocol):
        return type(self), (self._actor, self._actor_methods), None

class ActorClass:
    def __init__(self, modified_class):
        self._modified_class = modified_class

    def options(self):
        raise NotImplemented("Not yet implemented")

    def remote(self, *args, **kwargs):
        actor = _global_context.make_actor(self._modified_class, args, kwargs)
        actor_methods = {}
        for name, method in inspect.getmembers(self._modified_class, is_function_or_method):
            actor_methods[name] = {}
            for attr in ["__module__", "__name__", "__qualname__", "__doc__", "__annotations__"]:
                try:
                    value = getattr(method, attr)
                except AttributeError:
                    pass
                else:
                    actor_methods[name][attr] = value
        return ActorHandle(actor, actor_methods)


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

def get(futures):
    if isinstance(futures, _ray.Future):
        return _global_context.get(futures)

    results = []
    for future in futures:
        results.append(_global_context.get(future))
    return results