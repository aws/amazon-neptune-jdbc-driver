package software.amazon.neptune.opencypher.mock;

import lombok.AllArgsConstructor;
import java.util.concurrent.atomic.AtomicInteger;

public class MockOpenCypherNodes {
    private static final AtomicInteger ANNOTATION_IDX = new AtomicInteger();
    public static final MockOpenCypherNode LYNDON = getPerson("lyndon", "bauto");
    public static final MockOpenCypherNode VALENTINA = getPerson("valentina", "bozanovic");
    public static final MockOpenCypherNode VINNY = getKitty("vinny");
    public static final MockOpenCypherNode TOOTSIE = getKitty("tootsie");

    static String getNextAnnotation() {
        return String.format("a%d", ANNOTATION_IDX.getAndIncrement());
    }

    /**
     * Function to get a Person Node with the provided parameters.
     * @param firstName First name.
     * @param lastName Last name.
     * @return Person Node.
     */
    public static MockOpenCypherNode getPerson(final String firstName, final String lastName) {
        return new Person(firstName, lastName, getNextAnnotation());
    }


    /**
     * Function to get a Kitty Node with the provided parameters.
     * @param name Name.
     * @return Kitty Node.
     */
    public static MockOpenCypherNode getKitty(final String name) {
        return new Kitty(name, getNextAnnotation());
    }


    @AllArgsConstructor
    private static class Person implements MockOpenCypherNode {
        private final String firstName;
        private final String lastName;
        private final String annotation;

        @Override
        public String getInfo() {
            return String.format("Person {first_name: '%s', last_name: '%s'}", firstName, lastName);
        }

        @Override
        public String getAnnotation() {
            return annotation;
        }

        @Override
        public String getIndex() {
            return "Person (first_name, last_name)";
        }
    }

    @AllArgsConstructor
    private static class Kitty implements MockOpenCypherNode {
        private final String name;
        private final String annotation;

        @Override
        public String getInfo() {
            return String.format("Kitty {name: '%s'}", name);
        }

        @Override
        public String getAnnotation() {
            return annotation;
        }

        @Override
        public String getIndex() {
            return "Kitty (name)";
        }
    }
}
