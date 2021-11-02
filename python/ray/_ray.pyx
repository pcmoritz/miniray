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
                # current_actor = 
                (<PyObject**> actor)[0] = <PyObject*> result
            else:
                current_actor = (<PyObject**> actor)[0]
                args, kwargs = cloudpickle.loads(arg_data)
                # print("method_name", method_name)
                actor_object = <object> current_actor
                result = getattr(actor_object, method_name.decode())(*args, **kwargs)
        except Exception as err:
            error_happened[0] = True
            return cloudpickle.dumps(err)
        return cloudpickle.dumps(result)

cdef extern from "src/ray/ray.h" nogil:
    cdef cppclass CActorID" ActorID":
        pass

    cdef cppclass CContext" Context":
        CContext()
        CActorID StartActor(c_string (void**, const c_string &, const c_string &, c_bool* error_happened) nogil, const c_string& init_args_data)
        void Submit(CActorID actor_id, c_string& method_name, c_string& arg_data)

cdef class Context:
    cdef:
        shared_ptr[CContext] context

    def start_actor(self, python_class, args, kwargs):
        init_args_data = cloudpickle.dumps([python_class, args, kwargs])
        cdef c_string c_init_args_data = init_args_data
        cdef CActorID result
        with nogil:
            result = self.context.get().StartActor(call_actor_method, c_init_args_data)
        return <int64_t>(result)

    def submit(self, actor_id, method_name, c_string c_args_data):
        cdef c_string c_method_name = method_name
        cdef int64_t c_actor_id = actor_id
        with nogil:
             self.context.get().Submit(<CActorID>c_actor_id, c_method_name, c_args_data)

def context():
    cdef Context context = Context()
    context.context.reset(new CContext())
    return context