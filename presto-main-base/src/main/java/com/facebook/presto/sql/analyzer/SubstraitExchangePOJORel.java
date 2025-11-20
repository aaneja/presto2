/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.planner.plan.ExchangeNode;
import io.substrait.hint.Hint;
import io.substrait.relation.Extension;
import io.substrait.relation.ImmutableExtensionSingle;
import io.substrait.relation.Rel;

/**
 * A POJO representation of a Substrait Exchange relation.
 * This is a placeholder until we implement an ExchangeRel POJO for the Proto type
 * https://github.com/substrait-io/substrait-java/issues/153#issuecomment-1593869384
 */
public class SubstraitExchangePOJORel
{
    private final Rel input;
    private final Extension.SingleRelDetail detail;
    private final ExchangeNode.Type exchangeType;
    public SubstraitExchangePOJORel(Rel input, ExchangeNode.Type exchangeType)
    {
        this.input = input;
        this.detail = new SubstraitSingleRelExtensionDetail(exchangeType.name());
        this.exchangeType = exchangeType;
    }

    public Rel getAsRel()
    {
        return ImmutableExtensionSingle.builder()
                .input(input)
                .detail(detail)
                .deriveRecordType(detail.deriveRecordType(input))
                .build();
    }

    public Rel getAsRelWithHint(Hint hint)
    {
        return ImmutableExtensionSingle.builder()
                .input(input)
                .detail(detail)
                .deriveRecordType(detail.deriveRecordType(input))
                .build().withHint(hint);
    }

    @Override
    public String toString()
    {
        return "SubstraitExchangePOJORel{" +
                "input=" + input +
                ", detail=" + detail +
                ", exchangeType=" + exchangeType +
                '}';
    }
}
