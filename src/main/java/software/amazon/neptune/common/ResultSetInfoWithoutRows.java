package software.amazon.neptune.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.util.List;

@AllArgsConstructor
@Getter
public class ResultSetInfoWithoutRows {
    private final int rowCount;
    private final List<String> columns;
}
