package com.ccri.britt

import java.io.{File, ByteArrayOutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.codec.binary.Base64

object EncoderTest extends App {

  def encodeRec(rec: GenericRecord): Array[Byte] = {
    val schema = new Schema.Parser().parse(new File("/Users/Britt/dev/avroTest/src/main/resources/User.avsc"))
    val bos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(bos, null)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    writer.write(rec, encoder)
    encoder.flush()
    bos.toByteArray
  }

  def decodeRec(bytes: Array[Byte]): User = {
    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val reader = new SpecificDatumReader[User](User.getClassSchema)
    reader.read(null, decoder)
  }

  def encode64(rec: GenericRecord): String = {
    val b = encodeRec(rec)
    Base64.encodeBase64String(b)
  }

  def decode64(str: String): User = {
    val b = Base64.decodeBase64(str)
    decodeRec(b)
  }

  println("Hello")
  val user = new User()
  user.setName("Laura")
  user.setFavoriteNumber(5)
  val b = encode64(user)
  println( s"Encoded string $b")
  val user2 = decode64(b)
  println( s"Favorite number is ${user2.getFavoriteNumber} and name is ${user2.getName}")
  println("Goodbye")
}
