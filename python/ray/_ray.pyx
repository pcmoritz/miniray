# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from libcpp cimport bool as c_bool, nullptr
from libc.stdint cimport int64_t
from libcpp.string cimport string as c_string
from libcpp.memory cimport shared_ptr

from cpython.ref cimport PyObject, Py_INCREF

import cloudpickle

cdef c_string call_actor_method(void** actor, const c_string& method_name, const c_string& arg_data, c_bool* error_happened) nogil:
   cdef PyObject* current_actor
   with gil:
        try:
            if method_name == b"__init__":
                python_class, args, kwargs = cloudpickle.loads(arg_data)
                result = python_class(*args, **kwargs)
                Py_INCREF(result)
                (<PyObject**> actor)[0] = <PyObject*> result
            else:
                current_actor = (<PyObject**> actor)[0]
                args, kwargs = cloudpickle.loads(arg_data)
                actor_object = <object> current_actor
                result = getattr(actor_object, method_name.decode())(*args, **kwargs)
        except Exception as err:
            error_happened[0] = True
            return cloudpickle.dumps(err)
        return cloudpickle.dumps(result)

cdef extern from "src/ray/ray.h" nogil:
    cdef cppclass CFuture" Future":
        pass

    cdef cppclass CActor" Actor":
        shared_ptr[CFuture] Submit(c_string& method_name, c_string& arg_data)

    cdef cppclass CContext" Context":
        CContext()
        shared_ptr[CActor] MakeActor(c_string (void**, c_string&, const c_string &, c_bool* error_happened) nogil, c_string init_args_data)
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

    def submit(self, method_name, c_string c_args_data):
        cdef c_string c_method_name = method_name
        cdef shared_ptr[CFuture] future
        with nogil:
             future = self.actor.get().Submit(c_method_name, c_args_data)
        return make_future(future)

cdef make_actor(shared_ptr[CActor] actor):
    cdef Actor result = Actor()
    result.actor = actor
    return result

cdef class Context:
    cdef:
        shared_ptr[CContext] context

    def make_actor(self, python_class, args, kwargs):
        init_args_data = cloudpickle.dumps([python_class, args, kwargs])
        cdef c_string c_init_args_data = init_args_data
        cdef shared_ptr[CActor] actor
        with nogil:
            actor = self.context.get().MakeActor(call_actor_method, c_init_args_data)
        return make_actor(actor)

    def get(self, Future future):
        cdef c_string data
        with nogil:
            data = self.context.get().Get(future.future)
        return cloudpickle.loads(data)


def context():
    cdef Context context = Context()
    context.context.reset(new CContext())
    return context