package genio

/**
 * recursive types not welcome here. punks
 */

object GenIo {
  case class Type(name: String, fieldStr: String, children: Array[Type]) {
    val fields =
      if (fieldStr.length > 0)
        fieldStr split "," map (_ trim) map (_ split " ") map (x => (x(0), x(1))) toList
      else
        List()
  }
  def Type(name: String, fieldStr: String): Type = Type(name, fieldStr, Array[Type]())
  val types = Array(
    Type("NodeInfo",         "short id, int port, InetAddress addr"),
    Type("Rec",              "short dst, short via"),
    Type("Msg",              "short src, short version, short session", Array(
      Type("Join",           "InetAddress addr, int port"),
      Type("Init",           "short id, ArrayList<NodeInfo> members"),
      Type("Membership",     "ArrayList<NodeInfo> members, short numNodes, short yourId"),
      Type("RoutingRecs",    "ArrayList<Rec> recs"),
      Type("Ping",           "long time, NodeInfo info"),
      Type("Pong",           "long time"),
      Type("Measurements",   "Array<short> probeTable, Array<byte> inflation"),
      Type("MemberPoll",     ""),
      Type("PeeringRequest", "")
    ))
  )
  def allTypes(types: Array[Type]): Array[Type] =
    types ++ (
      for (subs <- types map (_ children) map allTypes; typ <- subs) yield typ
    )
  val names = allTypes(types) map (_ name)
  val name2type = Map((names zip allTypes(types)):_*)
  val fqns = Map(
    "InetAddress" -> "java.net.InetAddress",
    "ArrayList" -> "java.util.ArrayList"
  )
  val primitives = Map("int" -> "Int", "long" -> "Long", "byte" -> "Byte", "short" -> "Short")
  val castables = Map("Integer" -> "int")
  def isPrimitive(x: String) = primitives contains x
  def nested(x: String) = (x split "[<>]")(1)
  case class Primitive(s:String)
  val nameGen = new Iterator[String] {
    val i = Iterator from 0
    override def hasNext = false
    override def next = "x" + i.next
  }
  def genRead(typ: String, dest: String) {
    genRead(typ,dest,true)
  }
  def genRead(typ: String, dest: String, instantiate: Boolean) {
    println("{")
    typ match {
      case x if isPrimitive(x) =>
        if (x == "int") println(<p>{dest} = readInt(in);</p>.text)
        else println(<p>{dest} = in.read{primitives(x)}();</p>.text)
      case "Array<byte>" =>
        println(<p>
          {dest} = new byte[readInt(in)];
          in.read({dest});
        </p>.text)
      case x if x startsWith "Array<" => {
        println(<p>{dest} = new {nested(typ)}[readInt(in)];</p>.text)
        println("for (int i = 0; i < " + dest + ".length; i++) {")
        genRead(nested(typ), dest + "[i]")
        println("}")
      }
      case "InetAddress" => {
        genReadDecl("Array<byte>", "buf")
        println(<p>
        {dest} = InetAddress.getByAddress(buf);
        </p>.text)
      }
      case x if names contains x => {
        if (instantiate) println(<p>{dest} = new {typ}();</p>.text)
        for ((typ,name) <- name2type(x).fields) genRead(typ, dest + "." + name)
        if (name2parent contains typ) genRead(name2parent(typ).name, dest, false)
      }
      case x if x startsWith "ArrayList" => {
        println(<p>{dest} = new ArrayList&lt;{nested(typ)}&gt;();</p>.text)
        println( "for (int i = 0, len = readInt(in); i < len; i++) {" )
        genReadDecl(nested(typ), "x");
        println(dest + ".add(x);")
        println("}")
      }
      case x if castables contains x =>
        println(dest + " = readInt(in);")
      case _ => println("Other: " + typ) // throw new Exception("shit")
    }
    println("}")
  }
  def cleanType(typ: String): String =
    if (typ startsWith "Array<") cleanType(nested(typ)) + "[]"
    else typ
  def genDecl(typ: String, name: String) =
    println(cleanType(typ) + " " + name + ";")
  def genReadDecl(typ: String, dest: String) {
    genDecl(typ, dest)
    genRead(typ, dest)
  }
  def genWrite(typ: String, value: String) {
    typ match {
      case x if isPrimitive(x) => println(<p>out.write{primitives(typ)}({value});</p>.text)
      case "Array<byte>" => println(<p>out.writeInt({value}.length);out.write({value});</p>.text)
      case x if x startsWith "Array<" => {
        println(<p>out.writeInt({value}.length);</p>.text)
        println("for (int i = 0; i < " + value + ".length; i++) {")
        genWrite(nested(typ), value + "[i]");
        println("}")
      }
      case "InetAddress" =>
        println(<p>byte[] buf = {value}.getAddress();out.writeInt(buf.length);out.write(buf);</p>.text)
      case x if names contains x => {
        for ((typ,name) <- name2type(x).fields) genWrite(typ, value + "." + name)
        if (name2parent contains typ) genWrite(name2parent(typ).name, value)
      }
      case x if x startsWith "ArrayList" => {
        println(<p> out.writeInt({value}.size()); </p>.text)
        println("for (int i = 0; i < " + value + ".size(); i++) {")
        genWrite(nested(typ), value + ".get(i)")
        println("}")
      }
      case x if castables contains x =>
        println(<p>out.write{primitives(castables(x))}({value});</p>.text)
      case _ => println("Other: " + typ)
    }
  }
  def bindParent(parent: Option[Type], typ: Type): Array[(String,Type)] = {
    val intermed: Array[(String,Type)] = typ.children flatMap (x => bindParent(Some(typ), x))
    val head: Array[(String,Type)] = parent match {
      case Some(p) => Array((typ.name, p))
      case None => Array[(String,Type)]()
    }
    head ++ intermed
  }
  val name2parent = Map( types flatMap (x => bindParent(None,x)) : _* )
  def parent(name: String) = name2parent get name
  def main(args: Array[String]) {
//    println("""
//      package edu.cmu.neuron2;
//      import java.io.*;
//      import java.net.*;
//      import java.util.*;
//    """)
    for (c <- allTypes(types)) {
      val extendStr = parent(c.name) match {
        case Some(p) => "extends " + p.name
        case None => ""
      }
      println(<p>class {c.name} {extendStr} {{</p>.text)
      for ((typ,name) <- c.fields) genDecl(typ, name)
      println("}")
    }
    println("""
      class Serialization {
    """)
    println("""
      public void serialize(Object obj, DataOutputStream out) throws IOException {
      if (false) {}
      """)
    for ((c,i) <- allTypes(types) zipWithIndex) {
      println(<p>else if (obj.getClass() == {c.name}.class) {{</p>.text)
      println(<p>{c.name} casted = ({c.name}) obj; out.writeInt({i});</p>.text)
      genWrite(c.name, "casted")
      println("}")
    }
    println("}")
    println("""
      public Object deserialize(DataInputStream in) throws IOException {
      switch (readInt(in)) {
    """)
    for ((c,i) <- allTypes(types) zipWithIndex) {
      println(<p>case {i}: {{ // {c.name}</p>.text)
      genReadDecl(c.name, "obj")
      println("return obj;}")
    }
    println("""
    default:throw new RuntimeException("unknown obj type");}}

    private byte[] readBuffer = new byte[4];

    public int readInt(DataInputStream dis) throws IOException {
      dis.readFully(readBuffer, 0, 4);
      return (
        ((int)(readBuffer[0] & 255) << 24) +
        ((readBuffer[1] & 255) << 16) +
        ((readBuffer[2] & 255) <<  8) +
        ((readBuffer[3] & 255) <<  0));
    }

    /*
    public static void main(String[] args) throws IOException {
{
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      Pong pong = new Pong();
      pong.src = 2;
      pong.version = 3;
      pong.time = 4;
      serialize(pong, out);
      byte[] buf = baos.toByteArray();
      System.out.println(buf.length);
      Object obj = deserialize(new DataInputStream(new ByteArrayInputStream(buf)));
      System.out.println(obj);
}

{
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);

      Measurements m = new Measurements();
      m.src = 2;
      m.version = 3;
      m.membershipList = new ArrayList<Integer>();
      m.membershipList.add(4);
      m.membershipList.add(5);
      m.membershipList.add(6);
      m.ProbeTable = new long[5];
      m.ProbeTable[1] = 7;
      m.ProbeTable[2] = 8;
      m.ProbeTable[3] = 9;

      serialize(m, out);
      byte[] buf = baos.toByteArray();
      System.out.println(buf.length);
      Object obj = deserialize(new DataInputStream(new ByteArrayInputStream(buf)));
      System.out.println(obj);
}
{
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  DataOutputStream out = new DataOutputStream(baos);

  Membership m = new Membership();
  m.src = 2;
  m.version = 3;
  m.members = new ArrayList<NodeInfo>();
  NodeInfo n1 = new NodeInfo();
  n1.addr = InetAddress.getLocalHost();
  n1.port = 4;
  n1.id = 5;
  m.members.add(n1);
  NodeInfo n2 = new NodeInfo();
  n2.addr = InetAddress.getByName("google.com");
  n2.port = 6;
  n2.id = 7;
  m.members.add(n2);
  m.numNodes = 8;

  serialize(m, out);
  byte[] buf = baos.toByteArray();
  System.out.println(buf.length);
  Object obj = deserialize(new DataInputStream(
    new ByteArrayInputStream(buf)));
  System.out.println(obj);
}
    }*/
    }""")
  }
}



