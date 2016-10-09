package org.judal

import java.lang.AutoCloseable

object Using {
  
  def using[T <% AutoCloseable](resource: T)(block: => Unit) {
    try {
      block
    }
    finally {
      if (resource!=null) resource.close
    }
  }
}