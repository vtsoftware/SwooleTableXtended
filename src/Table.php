<?php declare(strict_types=1);
namespace VtSoftware\SwooleXtended;

use \Closure;
use \Swoole\Table AS SwooleTable;

class Table {
  const COL_SIZE_INT8 = 1;
  const COL_SIZE_INT16 = 2;
  const COL_SIZE_INT64 = 8;
  const COL_SIZE_INT32 = 4;

  private String $indexColumnName = '';
  private SwooleTable $instance;

  public static function new(int $size = 1024): static {
    return new static($size);
  }

  public function __construct(int $size = 1024) {
    $this->instance = new SwooleTable($size);
  }

  public function columns(array $cols): static {
    foreach ($cols as $colKey => $colPrefs) {
      switch ($colPrefs['type']) {
        case 'int': $type = SwooleTable::TYPE_INT; break;
        case 'string': $type = SwooleTable::TYPE_STRING; break;
        case 'float': $type = SwooleTable::TYPE_FLOAT; break;
      }

      $this->instance->column($colKey, $type, $colPrefs['size']);
    }
    $this->instance->create();
    return $this;
  }
  public function indexColumn(String $index): static {
    $this->indexColumnName = $index;
    return $this;
  }
  public function addRow(array $row): static {
    $this->instance->set('index_'.$row[$this->indexColumnName], $row);
    return $this;
  }
  public function addRows(array $rows): static {
    foreach ($rows as $row) {
      $this->instance->addRow($row);
    }
    return $this;
  }
  public function get(): array|String|bool {
    $argc = \func_num_args();
    $args = \func_get_args();

    if ($argc == 1) {
      if ($args[0] instanceof Closure) {
        foreach ($this->instance as $rowKey => $row) {
          if ($args[0]($row)) {
            return $row;
          }
        }
      } else {
        foreach ($this->instance as $rowKey => $row) {
          if ('index_'.$args[0] == $rowKey) {
            return $row;
          }
        }
      }
    } else if ($argc == 2) {
      foreach ($this->instance as $rowKey => $row) {
        if ($row[$args[0]] == $args[1]) {
          return $row;
        }
      }
    } else if ($argc == 3) {
      foreach ($this->instance as $rowKey => $row) {
        if ($row[$args[0]] == $args[1]) {
          return $row[$args[2]];
        }
      }
    }

    return false;
  }
  public function getWhere(): array {
    $argc = \func_num_args();
    $args = \func_get_args();

    $rows = array();

    if ($argc == 1 && $args[0] instanceof Closure) {
      $callback = $args[0];

      foreach ($this->instance as $rowKey => $row) {
        if (!$callback($row)) {
          continue;
        }

        $rows[] = $row;
      }
    } else if ($argc == 2) {
      foreach ($this->instance as $rowKey => $row) {
        if ($row[$args[0]] == $args[1]) {
          $rows[] = $row;
        }
      }
    } else if ($argc == 3) {
      foreach ($this->instance as $rowKey => $row) {
        if ($row[$args[0]] == $args[1]) {
          $rows[] = $row[$args[2]];
        }
      }
    }

    return $rows;
  }
  public function delete(): bool {
    $argc = \func_num_args();
    $args = \func_get_args();

    if ($argc == 1) {
      $this->instance->del('index_'.$args[0]);
      return true;
    } else if ($argc == 2) {
      foreach ($this->instance as $rowKey => $row) {
        if ($row[$args[0]] == $args[1]) {
          $this->instance->del($rowKey);
          return true;
        }
      }
    }

    return false;
  }
  public function exists(String $value): bool {
    return $this->instance->exists('index_'.$value);
  }
  public function getRows(): array {
    $rows = array();

    foreach ($this->instance as $rowKey => $row) {
      $rows[$rowKey] = $row;
    }

    return $rows;
  }
  public function set(String $indexValue, String $column, String $value) {
    $row = $this->get('index_'.$indexValue);
    $row[$column] = $value;
    $this->instance->set('index_'.$indexValue, $row);
  }
}
