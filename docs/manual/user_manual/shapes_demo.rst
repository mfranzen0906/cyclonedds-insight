..
   Copyright(c) 2024 Sven Trittler

   This program and the accompanying materials are made available under the
   terms of the Eclipse Public License v. 2.0 which is available at
   http://www.eclipse.org/legal/epl-2.0, or the Eclipse Distribution License
   v. 1.0 which is available at
   http://www.eclipse.org/org/documents/edl-v10.php.

   SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause


Shapes Demo
===========

|var-project| includes a shapes demo application that visualizes various shapes being published over DDS.
This demo is a well known demo application for DDS implementations.

1. Go to "Menu" -> "Shapes Demo" to start the shapes demo window
2. Publish or subscribe to shapes using the buttons on the left

.. image:: ../_static/images/shapes_demo.png

3. Manage the shapes using the "Manage Shapes" button

.. image:: ../_static/images/shapes_demo_manage.png

Web API with Shapes Demo
------------------------

In Settings, enable the Web API and choose a port. It listens on the local
computer only. Open ``http://127.0.0.1:8080/docs`` (replace 8080 with your
chosen port) for Swagger UI with live reader subscriptions. The page loads
Swagger UI assets from the internet.

Create a ShapeType reader and writer in Insight for the shape topic. Then open
``/api/paths`` to find their URLs. Paths use the IDL struct name, such as
``/api/readers/ShapeType`` and ``/api/writers/ShapeType``. If more than one
endpoint uses the same struct, the path also includes the topic and, when
needed, the domain or an ordinal. The topic is omitted when it has the same
name as the struct. For example, two ``HelloWorld`` readers on a topic named
``HelloWorld`` in domains 1 and 2 use ``/api/readers/HelloWorld/domain/1``
and ``/api/readers/HelloWorld/domain/2``. A reader's GET URL streams samples as
``text/event-stream``. Use Subscribe inside its Swagger GET operation to view
them. Disconnect clears the event display; subscribing again starts with a
fresh display. New subscribers receive the cached latest sample only when the
Insight reader uses Transient or TransientLocal durability. Volatile readers
stream samples received after subscription.

Send a JSON object to the writer's POST URL. For a ShapeType sample, ``color``
is the keyed field and must be included; the other fields use DDS defaults when
omitted. For example, using the writer path from ``/api/paths``::

    curl -X POST http://127.0.0.1:8080/api/writers/ShapeType \
      -H 'Content-Type: application/json' \
      -d '{"color":"BLUE","x":72,"y":80,"shapesize":30}'

The API returns ``queued`` after handing a validated sample to Insight's DDS
writer thread. Reader and writer URLs disappear when those Insight endpoints
are deleted; stopped writers cannot accept POST requests.
