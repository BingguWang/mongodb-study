package model

import "go.mongodb.org/mongo-driver/bson/primitive"

type Class struct {
	Id        *primitive.ObjectID
	ClassId   uint       `json:"classId"`
	ClassName string     `json:"className"`
	ClassDesc *ClassDesc `json:"classDesc"`
	Years     int        `json:"years"`
	Brief     string     `json:"brief"`
}
type ClassDesc struct {
	StuCount     int `json:"stuCount"`
	TeacherCount int `json:"teacherCount"`
}
