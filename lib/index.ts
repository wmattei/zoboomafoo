import {
  CreateIndexesOptions,
  Document,
  Filter,
  IndexDirection,
  InsertOneOptions,
  MongoClient,
  ObjectId,
  UpdateFilter,
  UpdateOptions,
} from "mongodb";
import { ZodObject, ZodRawShape, ZodSchema, input, output, z } from "zod";

export class ZoboomafooValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ZoboomafooValidationError";
  }
}

export class Zoboomafoo {
  private static client: MongoClient;
  private static queue: Array<() => Promise<void> | void> = [];

  static state = "disconnected";
  static async connect(url: string) {
    this.state = "connecting";
    if (!Zoboomafoo.client) {
      Zoboomafoo.client = new MongoClient(url);
      Zoboomafoo.client.on("open", async () => {
        this.state = "connected";
        for (const func of this.queue) {
          await func();
        }
        this.queue = [];
      });

      await Zoboomafoo.client.connect();
    }
  }

  static get db() {
    return Zoboomafoo.client.db();
  }

  static addToQueue(func: () => void) {
    this.queue.push(func);
  }
}

type IBaseSchema = {
  _id: ObjectId;
  createdAt: Date;
  updatedAt: Date;
  deletedAt?: Date;
};

type DocumentDefinition<T extends ZodObject<any>> = output<T> & IBaseSchema;

interface IModel<T extends ZodObject<any>> {
  insertOne(
    data: input<T>,
    insertOptions?: InsertOneOptions
  ): Promise<DocumentDefinition<T>>;
  updateOne(
    filter: Filter<input<T>>,
    data: UpdateFilter<input<T>>,
    updateOptions?: UpdateOptions
  ): Promise<DocumentDefinition<T> | null>;
  deleteOne(filter: Filter<input<T>>): Promise<boolean>;
  findById: (id: string | ObjectId) => Promise<DocumentDefinition<T> | null>;
  findOne: (filter: Filter<input<T>>) => Promise<DocumentDefinition<T> | null>;

  defineIndexes: (
    indexes: Array<
      [
        Record<keyof output<T> & IBaseSchema, IndexDirection>,
        CreateIndexesOptions
      ]
    >
  ) => Promise<void>;
}

type ModelOptions = {
  collectionName: string;
  softDelete?: boolean;
};

function parseModel<T extends ZodSchema>(schema: T, data: input<T>): output<T> {
  try {
    return schema.parse(data);
  } catch (error: any) {
    throw new ZoboomafooValidationError(error.message);
  }
}

class ZoboomafooModel<T extends ZodObject<any>> implements IModel<T> {
  constructor(private schema: T, private options: ModelOptions) {}
  async insertOne(data: input<T>, insertOptions?: InsertOneOptions) {
    const parsedData = parseModel(this.schema, data);
    const collection = Zoboomafoo.db.collection(this.options.collectionName);
    const decoratedData = {
      ...parsedData,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    const { insertedId } = await collection.insertOne(
      decoratedData,
      insertOptions
    );
    return { ...decoratedData, _id: insertedId };
  }

  async updateOne(
    filter: Filter<input<T>>,
    data: UpdateFilter<input<T>>,
    updateOptions?: UpdateOptions
  ): Promise<DocumentDefinition<T> | null> {
    const payload = data;
    // TODO validate schema on other operations
    if (data.$set) {
      const partialSchema = this.schema.pick(
        Object.keys(data.$set!).reduce(
          (acc, key) => ({ ...acc, [key]: true }),
          {}
        )
      );
      const parsedData = parseModel(partialSchema, data.$set!) as Partial<
        DocumentDefinition<T>
      >;
      payload.$set = parsedData;
    }

    const collection = Zoboomafoo.db.collection(this.options.collectionName);
    await collection.updateOne(
      filter as Filter<Document>,
      payload as UpdateFilter<Document>,
      updateOptions
    );
    return data as DocumentDefinition<T>;
  }

  async deleteOne(filter: Filter<input<T>>): Promise<boolean> {
    const collection = Zoboomafoo.db.collection(this.options.collectionName);
    if (!this.options.softDelete) {
      const result = await collection.deleteOne(filter as Filter<Document>);
      return result.deletedCount === 1;
    }
    const result = await collection.updateOne(filter as Filter<Document>, {
      deletedAt: new Date(),
    });
    return result.modifiedCount === 1;
  }

  async findById(id: string | ObjectId) {
    const collection = Zoboomafoo.db.collection(this.options.collectionName);
    const filter = { _id: new ObjectId(id) };
    const res = await collection.findOne(filter);
    return res as DocumentDefinition<T>;
  }

  async findOne(filter: Filter<input<T>>) {
    const collection = Zoboomafoo.db.collection(this.options.collectionName);
    const res = await collection.findOne(filter as Filter<Document>);
    return res as DocumentDefinition<T>;
  }

  async defineIndexes(
    indexes: Array<
      [
        Record<keyof output<T> & IBaseSchema, IndexDirection>,
        CreateIndexesOptions
      ]
    >
  ) {
    const handler = async () => {
      const collection = Zoboomafoo.db.collection(this.options.collectionName);
      const currentIndexes = await collection.indexes();
      const indexesToAdd: Array<{
        key: Record<keyof output<T> & IBaseSchema, IndexDirection>;
        options: CreateIndexesOptions;
      }> = [];
      const indexesToDelete: string[] = [];
      for (const [index, options] of indexes) {
        const foundIndex = currentIndexes.find(
          (i) => JSON.stringify(i.key) === JSON.stringify(index)
        );
        if (!foundIndex) {
          indexesToAdd.push({ key: index, options });
        }
      }

      for (const currentIndex of currentIndexes) {
        if (currentIndex.name === "_id_") continue;
        const foundIndex = indexes.find(
          ([index]) =>
            JSON.stringify(index) === JSON.stringify(currentIndex.key)
        );
        if (!foundIndex) {
          indexesToDelete.push(currentIndex.name!);
        }
      }

      for (const index of indexesToDelete) {
        await collection.dropIndex(index);
      }
      for (const { key, options } of indexesToAdd) {
        await collection.createIndex(key, options);
      }
    };
    if (Zoboomafoo.state !== "connected") {
      Zoboomafoo.addToQueue(handler);
      return;
    }

    await handler();
  }

  public type: output<T> = {};
}

export function Model<T extends ZodRawShape>(shape: T, options: ModelOptions) {
  const model = new ZoboomafooModel(z.object(shape), options);
  return model;
}

export const ObjectIdSchema = z.custom<string | ObjectId>().transform((value) => {
  return new ObjectId(value);
});

export type ModelType<T extends ZoboomafooModel<any>> = T["type"] & IBaseSchema;
