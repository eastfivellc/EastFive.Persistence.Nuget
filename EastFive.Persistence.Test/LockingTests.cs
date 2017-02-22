using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using BlackBarLabs.Persistence.Azure.StorageTables;
using BlackBarLabs.Extensions;
using System.Linq;
using BlackBarLabs;
using System.Threading.Tasks;

namespace EastFive.Persistence.Test
{
    [TestClass]
    public class LockingTests
    {
        [TestMethod]
        public async Task LockingStressTest()
        {
            var azureStorageRepository = AzureStorageRepository.CreateRepository("EastFive.Persistence.ASTConnectionString");
            var docToContent = new TestDocument()
            {
                Value = 0,
            };
            var docId = Guid.NewGuid();
            await azureStorageRepository.CreateAsync(docId, docToContent,
                () => true,
                () =>
                {
                    Assert.Fail("Failed to create test doc");
                    return false;
                });
            var count = 100;
            var totals = await Enumerable
                    .Range(1, count)
                    .Select(
                        async i =>
                        {
                            try
                            {
                                return await azureStorageRepository.LockedUpdateAsync<TestDocument, int>(
                                    docId,
                                    doc => doc.Lock,
                                    doc => (!((i % 70) == 0)).ToTask(),
                                    async (doc, unlockAndSave, unlock) =>
                                    {
                                        if ((i % 22) == 0)
                                            throw new ValueException(1);
                                        if ((i % 5) == 0)
                                        {
                                            await unlock();
                                            if ((i % 15) == 0)
                                                throw new ValueException(1);
                                            return 1;
                                        }
                                        var value = doc.Value + 1;
                                        await unlockAndSave(
                                            (currentDoc, save) =>
                                            {
                                                doc.Value = value;
                                                var x = save(doc);
                                                if ((i % 3) == 0)
                                                    throw new ValueException(0);
                                                return x;
                                            });
                                        if ((i % 4) == 0)
                                            throw new ValueException(0);
                                        return 0;
                                    },
                                    () => 1,
                                    () =>
                                    {
                                        Assert.Fail("Document failed to locate");
                                        return 1;
                                    });
                            }
                            catch (ValueException ex)
                            {
                                return ex.Value;
                            }
                        })
                       .WhenAllAsync();
            var total = totals.Sum();
            Assert.AreEqual(count, total + (await azureStorageRepository.FindByIdAsync(docId,
                (TestDocument doc) => doc.Value,
                () => 0)));
        }

        private class ValueException : Exception
        {
            public int Value;

            public ValueException(int v)
            {
                this.Value = v;
            }
        }
    }
}
