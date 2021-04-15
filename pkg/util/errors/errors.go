package errors

import (
	"reflect"

	gomysql "github.com/siddontang/go-mysql/mysql"
)

// copied from errors.Is(), but replace Unwrap() with Cause()
func Is(err, target error) bool {
	if target == nil {
		return err == target
	}

	isComparable := reflect.TypeOf(target).Comparable()
	for {
		if isComparable && err == target {
			return true
		}
		if x, ok := err.(interface{ Is(error) bool }); ok && x.Is(target) {
			return true
		}
		// TODO: consider supporing target.Is(err). This would allow
		// user-definable predicates, but also may allow for coping with sloppy
		// APIs, thereby making it easier to get away with them.
		if err = Cause(err); err == nil {
			return false
		}
	}
}

func CheckAndGetMyError(err error) (*gomysql.MyError, bool) {
	if err == nil {
		return nil, false
	}

	for {
		if err1, ok := err.(*gomysql.MyError); ok {
			return err1, true
		}
		if err = Cause(err); err == nil {
			return nil, false
		}
	}
}

func Cause(err error) error {
	u, ok := err.(interface {
		Cause() error
	})
	if !ok {
		return nil
	}
	return u.Cause()
}
