<?php
/**
 * Created by PhpStorm.
 * User: matthes
 * Date: 02.09.16
 * Time: 20:57
 */

namespace DbAkl\Com\FSql;


class FSqlExpr
{

    public function or() {

    }

    public function eq() {

    }

    public function like() {

    }

    // Example - $qb->expr()->neq('u.id', '?1') => u.id <> ?1
    public function neq($x, $y) {

    } // Returns Expr\Comparison instance

    // Example - $qb->expr()->lt('u.id', '?1') => u.id < ?1
    public function lt($x, $y) {

    } // Returns Expr\Comparison instance

    // Example - $qb->expr()->lte('u.id', '?1') => u.id <= ?1
    public function lte($x, $y) {

    } // Returns Expr\Comparison instance

    // Example - $qb->expr()->gt('u.id', '?1') => u.id > ?1
    public function gt($x, $y) {

    } // Returns Expr\Comparison instance

    // Example - $qb->expr()->gte('u.id', '?1') => u.id >= ?1
    public function gte($x, $y) {

    } // Returns Expr\Comparison instance

    // Example - $qb->expr()->isNull('u.id') => u.id IS NULL
    public function isNull($x) {

    } // Returns string

    // Example - $qb->expr()->isNotNull('u.id') => u.id IS NOT NULL
    public function isNotNull($x) {

    } // Returns string


}