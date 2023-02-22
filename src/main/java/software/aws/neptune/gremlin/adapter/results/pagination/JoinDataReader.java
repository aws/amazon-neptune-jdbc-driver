/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.aws.neptune.gremlin.adapter.results.pagination;

import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JoinDataReader implements GetRowFromMap {
    private final List<Pair<String, String>> tableColumnList = new ArrayList<>();

    public JoinDataReader(final Map<String, List<String>> tablesColumns) {
        tablesColumns.forEach((key, value) -> value.forEach(column -> tableColumnList.add(new Pair<>(key, column))));
    }

    @Override
    public Object[] execute(final Map<String, Object> map) {
        final Object[] row = new Object[tableColumnList.size()];
        int i = 0;
        for (final Pair<String, String> tableColumn : tableColumnList) {
            final Optional<String> tableKey =
                    map.keySet().stream().filter(key -> key.equalsIgnoreCase(tableColumn.left)).findFirst();
            if (!tableKey.isPresent()) {
                row[i++] = null;
                continue;
            }

            final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(tableColumn.right)).findFirst();
            if (!columnKey.isPresent()) {
                row[i++] = null;
                continue;
            }
            row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
        }
        return row;
    }
}
