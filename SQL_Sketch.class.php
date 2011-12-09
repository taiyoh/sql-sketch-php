<?php

class SQL_Sketch
{
    protected $select  = array();
    protected $join    = array();
    protected $joinTables = array();
    protected $where   = array();
    protected $groupby = null;
    protected $orderby = array();

    protected $limit  = null;
    protected $offset = null;

    protected $table = null;

    public function __construct($table)
    {
        $this->setTable($table);
        $this->init();
    }

    protected function init()
    {
        $this->select  = array();
        $this->join    = array();
        $this->where   = array();
        $this->groupby = null;
        $this->orderby = array();

        $this->joinTables = array();
        $this->joinTables[$this->table] = true;
    }

    public function setTable($table)
    {
        $this->table = $table;
    }

    public function addSelectColumn($clm, $as = null)
    {
        $this->select[] = (is_null($as)) ? $clm : "{$clm} AS {$as}";
        return $this;
    }

    public function addJoin($src1, $src2, $join)
    {
        $src1_l = explode('.',$src1);
        $src2_l = explode('.',$src2);

        $join_tbl = (!isset($this->joinTables[$src1_l[0]]))
            ? $src1_l[0]
            : $src2_l[0];

        $this->join[] = "{$join} {$join_tbl} ON ({$src1} = {$src2})";
        $this->joinTables[$join_tbl] = true;

        return $this;
    }

    public function addGroupByColumn($group)
    {
        $this->groupby = $group;
        return $this;
    }

    public function addOrderByColumn($clm, $asc_desc = null)
    {
        $this->orderby[] = (is_null($asc_desc))
            ? $clm
            : "{$clm} {$asc_desc}";
        return $this;
    }

    public function addAscendingOrderByColumn($clm)
    {
        $this->addOrderByColumn($clm, 'ASC');
        return $this;
    }

    public function addDescendingOrderByColumn($clm)
    {
        $this->addOrderByColumn($clm, 'DESC');
        return $this;
    }

    public function addCond(SQL_Sketch_Condition $cond)
    {
        $this->where[] = $cond;
        return $this;
    }

    public function getCond($label, $value, $op = '=')
    {
        return new SQL_Sketch_Condition($label, $value, $op);
    }

    public function add($label, $value, $op = '=')
    {
        $this->addCond(new SQL_Sketch_Condition($label, $value, $op));
        return $this;
    }

    public function setLimit($limit)
    {
        $this->limit = (int)$limit;
        return $this;
    }

    public function setOffset($offset)
    {
        $this->offset = (int)$offset;
        return $this;
    }

    public function select($options = array())
    {
        if (!empty($options)) {
            $this->parseOption($options);
        }
        $table = $this->table;
        $columns = '*';
        if (!empty($this->select)) {
            $columns = implode(', ', $this->select);
        }
        $select = "SELECT {$columns} FROM {$table}";

        if (!empty($this->join)) {
            $joins = implode(' ', $this->join);
            $select .= " {$joins}";
        }

        $wheres = array();
        $binds  = array();
        foreach($this->where as $wh) {
            $ret = $wh->asSQL();
            $wheres[] = $ret[0];
            $binds    = array_merge($binds, $ret[1]);
        }

        if (!empty($wheres)) {
            $select .= " WHERE " . implode(' AND ', $wheres);
        }

        if (!is_null($this->groupby)) {
            $select .= " GROUP BY " . $this->groupby;
        }

        if (!empty($this->orderby)) {
            $select .= " ORDER BY " . implode(', ', $this->orderby);
        }

        if (!is_null($this->limit)) {
            $limit  = array($this->limit);
            if (!is_null($this->offset)) {
                array_unshift($this->offset, $limit);
            }
            $select .= " LIMIT ". implode(',', $limit);
        }
        
        return array($select, $binds);
    }

    public function count($id = '*', $options = array())
    {
        $as = null;
        if (isset($options['as'])) {
            $as = $options['as'];
            unset($options['as']);
        }
        else if (is_string($options)) {
            $as = $options;
            $options = array();
        }
        $select = $this->select;
        $cnt = (is_null($as)) ? "COUNT({$id})" : "COUNT({$id}) AS {$as}";
        $this->select = array($cnt);

        $ret = $this->select($options);

        $this->select = $select;

        return $ret;
    }

    public function update($sets, $options = array())
    {
        $table  = $this->table;
        $update = "UPDATE {$table}";

        if (!empty($options)) {
            $this->parseOption($options);
        }
        
        $pl = SQL_Sketch_Condition::$objectID++;
        $setlist = array();
        $binds = array();
        $count = 1;
        foreach($sets as $k => $v) {
            $pln = ":sp{$pl}_{$count}";
            $setlist[] = "{$k}={$pln}";
            $binds[$pln] = $v;
            ++$count;
        }
        $update .= " SET " . implode(', ', $setlist);

        $wheres = array();
        foreach($this->where as $wh) {
            $ret = $wh->asSQL();
            $wheres[] = $ret[0];
            $binds    = array_merge($binds, $ret[1]);
        }

        if (!empty($wheres)) {
            $update .= " WHERE " . implode(' AND ', $wheres);
        }

        return array($update, $binds);
    }
      
    public function insert(array $list, $bulk = array())
    {
        $table  = $this->table;
        $insert = "INSERT INTO {$table}";

        $binds = array();

        if (empty($bulk)) {
            $ref = (isset($list[0])) ? $list[0] : $list;
            $bulk = array_keys($ref);
        }
        $insert .= sprintf(' (%s)', implode(',', $bulk));
        if (isset($list[0])) {
            $values = array();
            foreach($list as $data) {
                $record = array();
                foreach($bulk as $k) {
                    $binds[]  = $data[$k];
                    $record[] = '?';
                }
                $values[] = sprintf('(%s)', implode(',',$record));
            }
            $insert .= " VALUES " . implode(', ', $values);
        }
        else {
            $record  = array();
            foreach($bulk as $k) {
                $binds[]  = $list[$k];
                $record[] = '?';
            }
            $insert .= sprintf(' VALUES (%s)',  implode(', ', $record));
        }
        return array($insert, $binds);
    }

    public function delete($options = array())
    {
        $table  = $this->table;
        $delete = "DELETE FROM {$table}";

        if (!empty($options)) {
            $this->parseOption($options);
        }
        
        $binds  = array();
        $wheres = array();
        foreach($this->where as $wh) {
            $ret = $wh->asSQL();
            $wheres[] = $ret[0];
            $binds    = array_merge($binds, $ret[1]);
        }

        if (!empty($wheres)) {
            $delete .= " WHERE " . implode(' AND ', $wheres);
        }

        return array($delete, $binds);
    }

    protected function parseOption($options)
    {
        $this->init();
        if (isset($options['conditions'])) {
            $this->parseConditions($options['conditions']);
        }
        if (isset($options['with'])) {
            $this->parseWith($options['with']);
        }
        if (isset($options['group'])) {
            $clm = (strpos($options['group'], '.') !== false)
                ? $options['group']
                : $this->table .'.'. $options['group'];
            $this->addGroupByColumn($clm);
        }
        if (isset($options['order'])) {
            foreach($options['order'] as $column => $asc_desc) {
                $clm = $this->table.'.'.$column;
                $this->addOrderByColumn($clm, strtoupper($asc_desc));
            }
        }
        if (isset($options['limit'])) {
            $this->setLimit($options['limit']);
        }
    }

    protected function parseConditions($conds)
    {
        foreach($conds as $label => $cond) {
            $l_label = strtolower($label);
            if ($l_label === '-and' || $l_label === '-or') {
                $c = null;
                foreach($cond as $_lbl => $cnd) {
                    list($op, $val) = each($cnd);
                    $op = strtolower($op);
                    $clm = $_lbl;
                    if (strpos($clm, '.') === false) {
                        $clm = $this->table .'.'.$clm;
                    }
                    if (is_null($c)) {
                        $c = $this->getCond($clm, $val, $op);
                    }
                    else {
                        if ($l_label === '-or') {
                            $c->addOr($clm, $val, $op);
                        }
                        else {
                            $c->addAnd($clm, $val, $op);
                        }
                    }
                }
                $this->addCond($c);
            }
            else {
                list($op, $val) = each($cond);
                $clm = $label;
                if (strpos($clm, '.') === false) {
                    $clm = $this->table .'.'.$clm;
                }
                $this->add($label, $val, strtolower($op));
            }
        }
    }

    protected function parseWith($withs)
    {
        foreach($withs as $column => $with) {
            list($join, $src) = each($with);
            $ref = $this->table . '.' . $column;
            $this->addJoin($ref, $src, strtolower($join));
        }
    }
}

class SQL_Sketch_Condition
{
    public static $objectID = 1;

    protected $label = null;
    protected $value = null;
    protected $operator = null;

    protected $meta_operator = null;

    protected $stash = array();

    private $mysqlmaker_objid = null;

    protected static $operators = array(
        '=' => '=',
        '<>' => '<>',
        '!=' => '!=',
        '<' => '<',
        '>' => '>',
        '<=' => '<=',
        '>=' => '>=',
        'like' => 'LIKE',
        'not_like' => 'NOT LIKE',
        'in' => 'IN',
        'not_in' => 'NOT IN',
        'between' => 'BETWEEN',
    );

    protected static $meta_operators = array(
        'and' => 'AND',
        'or'  => 'OR',
    );

    public function __construct($l, $v, $o = '=')
    {
        $this->label = $l;
        $this->value = $v;
        $this->stash = array();
        $_o = strtolower($o);
        if (!isset(self::$operators[$_o])) {
            throw Exception('NO OPERATORS!');
        }
        $this->operator      = self::$operators[$_o];
        $this->meta_operator = null;
        $this->mysqlmaker_objid = self::$objectID++;
    }

    public function getObjectId()
    {
        return $this->mysqlmaker_objid;
    }

    public function getOperator()
    {
        return $this->operator;
    }

    public function getLabel()
    {
        return $this->label;
    }
    
    public function getValue()
    {
        return $this->value;
    }

    public function addOr($label, $value, $op = '=')
    {
        if (is_null($this->meta_operator) || $this->meta_operator === 'OR') {
            $this->meta_operator = 'OR';
            $this->stash[] = new self($label, $value, $op);
        }
        return $this;
    }

    public function addAnd($label, $value, $op = '=')
    {
        if (is_null($this->meta_operator) || $this->meta_operator === 'AND') {
            $this->meta_operator = 'AND';
            $this->stash[] = new self($label, $value, $op);
        }
        return $this;
    }

    public function asUpdateSQL()
    {
        $pl = ":sp" . $this->mysqlmaker_objid;
        $ret = array('', array());
        if($this->meta_operator) {
            $list = array();
            $params = array();
            foreach($this->stash as $st) {
                $myret = $st->asUpdateSQL();
                $list[] = $myret[0];
                $params = array_merge($params, $myret[1]);
            }
            $ret[0] = implode(', ', $list);
            $ret[1] = $params;
            break;
        }
        else {
            $ret[0] = sprintf('%s = %s', $this->label, $pl);
            $ret[1] = array($pl => $this->value);
        }
        return $ret;
    }

    public function asSQL()
    {
        $pl = ":sp" . $this->mysqlmaker_objid;
        $ret = array('', array());
        switch($this->operator) {
        case null:
            break;
        case 'IN':
        case 'NOT IN':
            $values = array();
            $c = 1;
            foreach($this->value as $v) {
                $vl = "{$pl}_{$c}";
                $values[] = $vl;
                $ret[1][$vl] = $v;
                ++$c;
            }
            $ret[0] = sprintf('(%s %s (%s))', $this->label, $this->operator, implode(',',$values));
            break;
        case 'BETWEEN':
            $ret[1] = array(
                "{$pl}_1" => $this->value[0],
                "{$pl}_2" => $this->value[1],
            );
            $ret[0] = sprintf('(%s BETWEEN %s AND %s)', $this->label, "{$pl}_1", "{$pl}_2");
            break;
        default:
            $ret[0] = sprintf('(%s %s %s)', $this->label, $this->operator, $pl);
            $ret[1] = array($pl => $this->value);
        }

        if (!is_null($this->meta_operator)) {
            $list = array();
            $params = array();
            foreach($this->stash as $st) {
                $myret = $st->asSQL();
                $list[] = $myret[0];
                $params = array_merge($params, $myret[1]);
            }
            if ($ret[0]) {
                array_unshift($list, $ret[0]);
            }
            $ret[0] = sprintf('(%s)', implode(' ' .$this->meta_operator. ' ', $list));
            $ret[1] = array_merge($ret[1], $params);
        }
        
        return $ret;
    }
}
