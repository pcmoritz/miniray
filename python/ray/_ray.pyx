# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from libcpp cimport bool as c_bool, nullptr
from libc.stdint cimport int64_t
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport shared_ptr

from cpython.buffer cimport Py_buffer
from cpython.ref cimport Py_INCREF, Py_DECREF

import cloudpickle
import pickle
import sys

cdef extern from "src/ray/ray.h" nogil:
    cdef cppclass CFuture" Future":
        CSerializedObject data()

    cdef cppclass CActor" Actor":
        shared_ptr[CFuture] Submit(c_string& method_name, CSerializedObject serialized_args)

    cdef cppclass CSerializedObject" SerializedObject":
        CSerializedObject()
        c_string data
        c_vector[shared_ptr[CActor]] handles
        c_bool error

    cdef cppclass CContext" Context":
        CContext()
        shared_ptr[CActor] MakeActor(void*, CSerializedObject (void*, c_string&, CSerializedObject) nogil, CSerializedObject serialized_init_args)

cdef class Serializer:
    cdef bytes data
    cdef object buffers

    def __init__(self, object):
        self.data = b""
        self.buffers = []
        cloudpickle.CloudPickler(
            self, protocol=5, buffer_callback=self.buffers.append).dump(object)

    def write(self, data):
        self.data = data

cdef CSerializedObject serialize(value, error=False):
    # TODO: Support arbitrary buffers
    cdef Serializer serializer = Serializer(value)
    cdef CSerializedObject result
    cdef ActorHandle handle
    result.data = serializer.data
    for buffer in serializer.buffers:
        handle = buffer.raw().obj
        result.handles.push_back(handle.actor)
    result.error = error
    return result

cdef deserialize(CSerializedObject object):
    buffers = []
    cdef shared_ptr[CActor] handle
    for handle in object.handles:
        buffers.append(pickle.PickleBuffer(make_actor_handle(handle)))
    result = cloudpickle.loads(object.data, buffers=buffers)
    if object.error:
        raise result
    else:
        return result


cdef CSerializedObject actor_method(ActorData actor, const c_string& method_name, CSerializedObject serialized_args):
    if method_name == b"__shutdown__":
        # Deallocate the Actor container here
        Py_DECREF(actor)
        return CSerializedObject()
    try:
        # TODO: Move serialized_args
        args, kwargs = deserialize(serialized_args)
        if method_name == b"__init__":
            actor.instance = actor.python_class(*args, **kwargs)
            return CSerializedObject()
        else:
            result = getattr(actor.instance, method_name.decode())(*args, **kwargs)
            return serialize(result)
    except Exception as err:
        return serialize(err, error=True)

cdef CSerializedObject call_actor_method(void* actor, const c_string& method_name, CSerializedObject serialized_args) nogil:
    with gil:
        return actor_method(<ActorData>actor, method_name, serialized_args)

cdef class Future:
    cdef:
        shared_ptr[CFuture] future

cdef make_future(shared_ptr[CFuture] future):
    cdef Future result = Future()
    result.future = future
    return result

cdef class ActorData:
    cdef:
        object instance
        object python_class

    def __init__(self, python_class):
        self.instance = None
        self.python_class = python_class

def _actor_handle_from_buffer(buffer):
    return buffer.raw().obj

cdef class ActorHandle:
    cdef:
        shared_ptr[CActor] actor
        object __weakref__

    def submit(self, c_string method_name, args, kwargs):
        cdef CSerializedObject serialized_args = serialize([args, kwargs])
        cdef shared_ptr[CFuture] future
        with nogil:
             future = self.actor.get().Submit(method_name, serialized_args)
        return make_future(future)

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        buffer.obj = self
        buffer.readonly = 0

    def __releasebuffer__(self, Py_buffer *buffer):
        pass

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
          return _actor_handle_from_buffer, (pickle.PickleBuffer(self),), None

cdef make_actor_handle(shared_ptr[CActor] actor):
    cdef ActorHandle result = ActorHandle()
    result.actor = actor
    return result

cdef class Context:
    cdef:
        shared_ptr[CContext] context

    def make_actor(self, python_class, args, kwargs):
        cdef CSerializedObject serialized_init_args = serialize([args, kwargs])
        cdef shared_ptr[CActor] actor
        cdef ActorData actor_data = ActorData(python_class)
        Py_INCREF(actor_data)
        with nogil:
            actor = self.context.get().MakeActor(<void*>actor_data, call_actor_method, serialized_init_args)
        return make_actor_handle(actor)

    def get(self, Future future):
        cdef CSerializedObject data
        with nogil:
            data = future.future.get().data()
        return deserialize(data)


def context():
    cdef Context context = Context()
    context.context.reset(new CContext())
    return context