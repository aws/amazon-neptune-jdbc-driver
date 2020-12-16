package software.amazon.neptune.opencypher.mock;

public interface MockOpenCypherNode {
    /**
     * Info for CREATE functions.
     * Example: NodeType {node_param: 'node_value'}
     * @return Info for CREATE functions.
     */
    String getInfo();

    /**
     * Annotation for CREATE functions.
     * @return Annotation for CREATE functions.
     */
    String getAnnotation();

    /**
     * Index for CREATE INDEX functions.
     * Example: NodeType (node_param)
     * @return Index for CREATE INDEX functions.
     */
    String getIndex();
}
