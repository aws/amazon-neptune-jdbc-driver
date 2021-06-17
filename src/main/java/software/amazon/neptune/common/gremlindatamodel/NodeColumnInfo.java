/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.common.gremlindatamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class NodeColumnInfo {
    @Getter
    private final List<String> labels;
    @Getter
    private final List<Map<String, Object>> properties;

    @Override
    public boolean equals(final Object nodeColumnInfo) {
        if (!(nodeColumnInfo instanceof NodeColumnInfo)) {
            return false;
        }
        final NodeColumnInfo nodeInfo = (NodeColumnInfo) (nodeColumnInfo);
        return nodeInfo.labels.equals(this.labels) && nodeInfo.properties.equals(this.properties);
    }
}
