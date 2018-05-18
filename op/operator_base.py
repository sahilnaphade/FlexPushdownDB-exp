# -*- coding: utf-8 -*-
"""Operator support

"""


class Operator(object):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. consumers).

    """

    def __init__(self):
        """Constructs a new operator

        """

        self.producers = []
        self.consumers = []

        self.__completed = False

    def is_completed(self):
        """Accessor for completed status.

        :return: Boolean indicating whether the operator has completed or not.
        """
        return self.__completed

    def connect(self, consumer):
        """Utility method that appends the given consuming operators to this operators list of consumers and appends the
        given consumers producer to this operator. Shorthand for two add consumer, add producer calls.

        TODO: Not sure if this should be here, sometimes consuming operators don't need a ref to a producer or indeed
        may have multiple.

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """

        self.consumers.append(consumer)
        consumer.producers.append(self)

    def add_consumer(self, consumer):
        """Appends the given consuming operator to this operators list of consumers

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """

        self.consumers.append(consumer)

    def add_producer(self, producer):
        """Appends the given producing operator to this operators list of producers.

        :param producer: An operator that will produce the tuples for this operator.
        :return: None
        """

        self.producers.append(producer)

    def send(self, t):
        """Emits the given tuple to each of the connected consumers.

        :param t: The tuple to emit
        :return: None
        """

        for c in self.consumers:
            c.on_receive(t, self)

    def complete(self):
        """Sets the operator to complete, meaning it has completed what it needed to do. This includes marking the
        operator as completed and signalling to to connected operators that this operator has
        completed what it was doing.

        :return: None
        """

        if not self.is_completed():

            # print("{} | Complete".format(self.__class__.__name__))

            self.__completed = True

            for c in self.consumers:
                c.on_producer_completed(self)

            for p in self.producers:
                p.on_consumer_completed(self)

        else:
            raise Exception("Cannot complete an already completed operator")

    def on_producer_completed(self, _producer):
        """Handles a signal from producing operators that they have completed what they needed to do. This is useful in
        circumstances where a producer has no more tuples to supply (such as completion of a table scan). This is often
        overridden but this default implementation simply completes this operator.

        :param _producer: The producer that has completed
        :return: None
        """

        if not self.is_completed():
            self.complete()

    def on_consumer_completed(self, _consumer):
        """Handles a signal from consuming operators that they have completed what they needed to do. This is useful in
        circumstances where a consumer needs no more tuples (such as a top operator reaching the number of tuples it
        needs). This is often overridden but this default implementation simply simply completes this operator.

        :param _consumer: The consumer that has completed
        :return: None
        """

        if not self.is_completed():
            self.complete()