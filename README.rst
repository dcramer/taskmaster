Taskmaster
----------

**This is under development, and this README reflects the potential API**

Spawn a master::

    tm-master path.to.module:iterator_callback --host=0.0.0.0:3050 --key=foobar --size=10000

Spawn slaves::

    tm-slave path.to.module:handle_callback --host=127.0.0.1:3050 --key=foobar
