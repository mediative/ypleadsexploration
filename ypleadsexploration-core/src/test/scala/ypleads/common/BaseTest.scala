package ypleads.common

import org.scalatest.FlatSpec
import ypleads.common.Common.functions
import functions._

class CommonTest extends FlatSpec {

  "'Dominos'" should "be close enough to 'Dominos P'" in {
    assert(isCloseEnough("Dominos", "Dominos P"))
  }

  it should "be close enough to 'Dominos Pizza'" in {
    assert(isCloseEnough("Dominos", "Dominos Pizza"))
  }

  it should "be close enough to 'Dmino'" in {
    assert(isCloseEnough("Dominos", "Dmino"))
  }

  "'Dominos Pizza'" should "be close enough to 'Dominos P'" in {
    assert(isCloseEnough("Dominos Pizza", "Dominos P"))
  }

  it should "be close enough to 'Dmino Pzz'" in {
    assert(isCloseEnough("Dominos Pizza", "Dmino Pzz"))
  }

}
