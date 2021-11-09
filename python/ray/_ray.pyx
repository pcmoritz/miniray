# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from libcpp cimport bool as c_bool, nullptr
from libc.stdint cimport int64_t
from libcpp.string cimport string as c_string
from libcpp.memory cimport shared_ptr

import cloudpickle


cdef class FastPickler:
    cdef bytes result
    cdef object pickler

    def __init__(self):
        self.result = b""
        self.pickler = cloudpickle.CloudPickler(self, protocol=5)

    def write(self, data):
        self.result = data

def dumps(data):
    cdef FastPickler pickler = FastPickler()
    pickler.pickler.dump(data)
    return pickler.result


cdef c_string actor_method(Actor actor, const c_string& method_name, const c_string& arg_data, c_bool* error_happened):
    try:
        if method_name == b"__init__":
            python_class, args, kwargs = cloudpickle.loads(arg_data)
            actor.instance = python_class(*args, **kwargs)
            return b""
        else:
            args, kwargs = cloudpickle.loads(arg_data)
            result = getattr(actor.instance, method_name.decode())(*args, **kwargs)
            return dumps(result)
    except Exception as err:
        error_happened[0] = True
        return cloudpickle.dumps(err)

cdef c_string call_actor_method(void* actor, const c_string& method_name, const c_string& arg_data, c_bool* error_happened) nogil:
    with gil:
        return actor_method(<Actor>actor, method_name, arg_data, error_happened)

cdef extern from "src/ray/ray.h" nogil:
    cdef cppclass CFuture" Future":
        pass

    cdef cppclass CActor" Actor":
        shared_ptr[CFuture] Submit(c_string& method_name, c_string& arg_data)

    cdef cppclass CContext" Context":
        CContext()
        shared_ptr[CActor] MakeActor(void*, c_string (void*, c_string&, const c_string &, c_bool* error_happened) nogil, c_string init_args_data)
        c_string Get(const shared_ptr[CFuture]& future)

cdef class Future:
    cdef:
        shared_ptr[CFuture] future

cdef make_future(shared_ptr[CFuture] future):
    cdef Future result = Future()
    result.future = future
    return result

cdef class Actor:
    cdef:
        shared_ptr[CActor] actor
        object instance

cdef class ActorHandle:
    cdef:
        shared_ptr[CActor] actor

    def submit(self, c_string method_name, args, kwargs):
        cdef c_string args_data = dumps([args, kwargs])
        cdef shared_ptr[CFuture] future
        with nogil:
             future = self.actor.get().Submit(method_name, args_data)
        return make_future(future)

cdef make_actor_handle(shared_ptr[CActor] actor):
    cdef ActorHandle result = ActorHandle()
    result.actor = actor
    return result

cdef class Context:
    cdef:
        shared_ptr[CContext] context
        object actors

    def make_actor(self, python_class, args, kwargs):
        init_args_data = cloudpickle.dumps([python_class, args, kwargs])
        cdef c_string c_init_args_data = init_args_data
        cdef Actor actor = Actor()
        with nogil:
            actor.actor = self.context.get().MakeActor(<void*>actor, call_actor_method, c_init_args_data)
        self.actors.append(actor)
        return make_actor_handle(actor.actor)

    def get(self, Future future):
        cdef c_string data
        with nogil:
            data = self.context.get().Get(future.future)
        return cloudpickle.loads(data)


def context():
    cdef Context context = Context()
    context.context.reset(new CContext())
    context.actors = []
    return context