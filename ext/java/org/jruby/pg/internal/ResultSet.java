package org.jruby.pg.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jruby.pg.messages.*;

public class ResultSet {
  /**
   * Explain the status of the ResultSet
   */
  public enum ResultStatus {
    PGRES_EMPTY_QUERY,          /* empty query string was executed */
    PGRES_COMMAND_OK,           /* a query command that doesn't return anything */
    PGRES_TUPLES_OK,            /* a query command that returns tuples was executed */
    PGRES_COPY_OUT,             /* Copy Out data transfer in progress */
    PGRES_COPY_IN,              /* Copy In data transfer in progress */
    PGRES_BAD_RESPONSE,         /* an unexpected response was recv'd from the backend */
    PGRES_NONFATAL_ERROR,       /* notice or warning message */
    PGRES_FATAL_ERROR,          /* query failed */
    PGRES_COPY_BOTH,            /* Copy In/Out data transfer in progress */
    PGRES_SINGLE_TUPLE;         /* single tuple from larger resultset */

    public boolean isCopyStatus() {
      switch(this) {
      case PGRES_COPY_BOTH:
      case PGRES_COPY_IN:
      case PGRES_COPY_OUT:
        return true;
      default:
        return false;
      }
    }
  }

  private boolean binaryTuples;
  private CommandComplete cmdStatus;
  private ResultStatus status;
  private RowDescription descrption;
  private ParameterDescription parameterDescription;
  private final List<DataRow> rows = new ArrayList<DataRow>();
  private String errorMsg;
  private ErrorResponse error;

  public static ResultSet createWithStatus(ResultStatus status) {
    return new ResultSet().setStatus(status);
  }

  /**
   * Return the data rows in the result
   */
  public List<DataRow> getRows() {
    return Collections.unmodifiableList(rows);
  }

  /**
   * Return true if the ResultSet contain binary data, false if it
   * contains textual data
   */
  public boolean binaryTuples() {
    return binaryTuples;
  }

  /**
   * Return the row description of the result
   */
  public RowDescription getDescription() {
    return descrption;
  }

  /**
   * Return the error message associated with this ResultSet
   *
   * @return the error message, null if there are no errors
   *         in this ResultSet
   */
  public String getError() {
    return errorMsg;
  }

  /**
   * Get the value of an error field
   *
   * @return the value of the given error field, null if it couldn't
   *         be found or if there are no errors in the ResultStatus
   */
  public String getErrorField(byte fieldCode) {
    if(error == null) {
      return null;
    }
    return error.getFields().get(fieldCode);
  }

  /**
   * Return the status, see {@link ResultStatus} for more info
   */
  public ResultStatus getStatus() {
    return status;
  }

  /**
   * Returns the number and types of the parameters that are expected
   * by the prepared statement
   */
  public ParameterDescription getParameterDescription() {
    return parameterDescription;
  }

  /**
   * Return the status of the last command (i.e. non SELECT) that was
   * run. This information include the number of rows affected.
   */
  public CommandComplete getCmdStatus() {
    return cmdStatus;
  }

  public ResultSet copy() {
    ResultSet res = new ResultSet().setStatus(ResultStatus.PGRES_TUPLES_OK);
    res.binaryTuples = binaryTuples;
    res.cmdStatus = cmdStatus;
    res.status = status;
    res.descrption = descrption;
    res.parameterDescription = parameterDescription;
    return res;
  }

  // package visible methods to set the different fields of the ResultSet

  ResultSet() {
    // make the constructore package visible
  }

  ResultSet setBinaryTuples(boolean binaryTuples) {
    this.binaryTuples = binaryTuples;
    return this;
  }

  ResultSet appendErrorMessage(String msg) {
    if(errorMsg == null) {
      errorMsg = msg;
    } else {
      errorMsg += msg;
    }
    return this;
  }

  ResultSet setCmdStatus(CommandComplete cmdStatus) {
    this.cmdStatus = cmdStatus;
    return this;
  }

  ResultSet setDescription(RowDescription descrption) {
    this.descrption = descrption;
    return this;
  }

  ResultSet appendRow(DataRow row) {
    rows.add(row);
    return this;
  }

  ResultSet setErrorResponse(ErrorResponse error) {
    this.errorMsg = error.getErrorMessage();
    this.error = error;
    return this;
  }

  ResultSet setStatus(ResultStatus status) {
    this.status = status;
    return this;
  }

  ResultSet setParameterDescription(ParameterDescription parameterDescription) {
    this.parameterDescription = parameterDescription;
    return this;
  }
}
