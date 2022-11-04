package org.apache.spark.sql.connector.read.streaming;

/**
 * Mixed-in interface to notice to Spark that the offset instance is comparable with another
 * offset instance. Both DSv1 Offset and DSv2 Offset are eligible to extend this interface.
 *
 * Spark will leverage this interface to perform assertions, for example, asserting offset range.
 */
public interface ComparableOffset {
  /**
   * The result of comparison between two offsets.
   * <p>
   * - EQUAL: two offsets are equal<br/>
   * - GREATER: this offset is greater than the other offset<br/>
   * - LESS: this offset is less than the other offset<br/>
   * - UNDETERMINED: can't determine which is greater<br/>
   *   e.g. greater for some parts but less for some other parts<br/>
   * - NOT_COMPARABLE: both are not comparable offsets e.g. different types
   */
  enum CompareResult {
    EQUAL,
    GREATER,
    LESS,
    UNDETERMINED,
    NOT_COMPARABLE
  }

  /**
   * Compare this offset with given offset.
   *
   * @see CompareResult
   */
  CompareResult compareTo(ComparableOffset other);
}
